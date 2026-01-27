# Análise dos dados

Para o início da análise dos dados, será utilizado o arquivo local `clientes_sinteticos.csv`, detalhando a abordagem utilizada para cada um dos requisitos solicitados.

## Clientes com maior número de atualizações

Para esta etapa, será necessário utilizar o arquivo sem nenhum tratamento prévio, tornando possível identificar alterações ao longo do tempo para cada cliente identificado pelo campo **cod_cliente**.

A lógica utilizada para a resolução dessa dúvida se resume ao agrupamento de valores da coluna **cod_cliente** e contagem de ocorrências, permitindo identificar os 5 clientes que mais sofreram atualizações através de ordenação decrescente do conjunto de dados, como exibido abaixo.

``` txt
+-----------+----------+
|cod_cliente|update_qty|
+-----------+----------+
|878        |5         |
|479        |5         |
|396        |5         |
|855        |4         |
|980        |3         |
+-----------+----------+
```

## Idade média dos clientes

Para o cálculo da idade média dos clientes, foram aplicadas algumas etapas de preparação dos dados com o objetivo de garantir consistência e confiabilidade estatística resultado.

As principais etapas consideradas foram:

* **Remoção de registros duplicados:** Como a base contém múltiplos registros por cliente devido a atualizações cadastrais, foi necessário deduplicar os dados, mantendo apenas o registro mais recente de cada cliente. Essa etapa evita que clientes com maior número de atualizações tenham peso desproporcional no cálculo da média, eliminando vieses no resultado.

* **Adequação de tipos de dados:** A coluna dt_nascimento_cliente, originalmente representada como texto, foi convertida para o tipo de dado date. Essa conversão é fundamental para permitir operações temporais corretas e evitar inconsistências no cálculo da idade.

* **Cálculo da idade dos clientes:** A idade foi calculada dinamicamente a partir da diferença entre a data de processamento do pipeline e a data de nascimento de cada cliente. Esse cálculo garante que a idade reflita o estado mais atual dos dados no momento da execução do processamento.

Após a aplicação dessas etapas, foi analisada a faixa etária dos clientes, que variam de 18 a 81 anos, e desconsiderados registros com valores nulos ou inválidos para a data de nascimento, e então foi realizado o cálculo da idade média dos clientes sobre o conjunto de dados resultante.

``` txt
+------------------+
|average_client_age|
+------------------+
|             51.23|
+------------------+
```

## Considerações de performance

Após análise de logs, foram aplicadas as seguintes alterações para melhoria de performance da análise dos dados:

* **Redução do dataset:** Remover as colunas desnecessárias para as análises propostas.
  
* **Configuração Spark:** Dada a configuração do ambiente, adicionar configuração `.config("spark.sql.shuffle.partitions", "8")` auxiliou no controle de partições para a realização do shuffle.

* **Cache de dados:** Armazenar dados em cache para reduzir o custo de reutilização após o shuffle.

Com as três técnicas listadas acima, foi possível diminuir o tempo de execução de 20s para 12s, um ganho proximado de 40% de performance.
