"""
Unit tests for silver transformations.
"""

import pytest
from pyspark.sql import Row
from ETL.script import SilverTransformations

# Happy path
@pytest.mark.parametrize(
    "data",
    [
        Row(
            cod_cliente="1",
            nm_cliente="Esron Matos",
            nm_pais_cliente="Brasil",
            nm_cidade_cliente="São Paulo",
            nm_rua_cliente="Rua ABC",
            num_casa_cliente="123",
            num_telefone_cliente="(11)91234-5678",
            dt_nascimento_cliente="1996-09-01",
            dt_atualizacao="2026-01-28",
            tp_pessoa="PF",
            vl_renda="5000"
        )
    ]
)
def test_apply_silver_transformation(spark, data):

    df = spark.createDataFrame([data])
    result = SilverTransformations.apply(df)

    row = result.collect()[0]

    assert row.cod_cliente == "1"
    assert row.num_telefone_cliente == "(11)91234-5678"
    assert row.dt_nascimento_cliente.year == 1996
    assert row.vl_renda == 5000.00


# Casos de borda - deduplicação
@pytest.mark.parametrize(
    "data, expected_phone, expected_income", 
    [
        (
            [
                Row(
                    cod_cliente="1",
                    nm_cliente="Esron Matos",
                    nm_pais_cliente="Brasil",
                    nm_cidade_cliente="São Paulo",
                    nm_rua_cliente="Rua ABC",
                    num_casa_cliente="123",
                    num_telefone_cliente="(11)91234-5678",
                    dt_nascimento_cliente="1996-09-01",
                    dt_atualizacao="2025-01-28",
                    tp_pessoa="PF",
                    vl_renda="2500"
                ),
                Row(
                    cod_cliente="1",
                    nm_cliente="Esron Matos",
                    nm_pais_cliente="Brasil",
                    nm_cidade_cliente="São Paulo",
                    nm_rua_cliente="Rua ABC",
                    num_casa_cliente="123",
                    num_telefone_cliente="(11)94321-8765",
                    dt_nascimento_cliente="1996-09-01",
                    dt_atualizacao="2026-01-28",
                    tp_pessoa="PF",
                    vl_renda="5000"
                )
            ],
            "(11)94321-8765",
            5000.00
        )
    ]
)
def test_apply_silver_deduplication(spark, data, expected_phone, expected_income):

    df = spark.createDataFrame(data)
    result = SilverTransformations.apply(df)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].vl_renda == expected_income
    assert rows[0].num_telefone_cliente == expected_phone


# Casos de borda - telefone inválido
@pytest.mark.parametrize(
    "data, expected_phone", 
    [
        (
            Row(
                cod_cliente="1",
                nm_cliente="Esron Matos",
                nm_pais_cliente="Brasil",
                nm_cidade_cliente="São Paulo",
                nm_rua_cliente="Rua ABC",
                num_casa_cliente="123",
                num_telefone_cliente="(11)91234-5678",
                dt_nascimento_cliente="1996-09-01",
                dt_atualizacao="2026-01-28",
                tp_pessoa="PF",
                vl_renda="5000"
            ),
            "(11)91234-5678"
        ),
        (
            Row(
                cod_cliente="2",
                nm_cliente="Maria Silva",
                nm_pais_cliente="Brasil",
                nm_cidade_cliente="Rio de Janeiro",
                nm_rua_cliente="Rua XYZ",
                num_casa_cliente="456",
                num_telefone_cliente="11912345678",
                dt_nascimento_cliente="1990-05-15",
                dt_atualizacao="2026-01-27",
                tp_pessoa="PF",
                vl_renda="6000"
            ),
            None
        )

    ]
)
def test_apply_silver_invalid_phone(spark, data, expected_phone):

    df = spark.createDataFrame([data])
    result = SilverTransformations.apply(df)

    row = result.collect()[0]

    assert row.num_telefone_cliente == expected_phone


# Erro ou exceção - coluna faltando
@pytest.mark.parametrize(
    "data", 
    [
        Row(
            cod_cliente="1",
            nm_cliente="Esron Matos",
            nm_pais_cliente="Brasil",
            nm_cidade_cliente="São Paulo",
            nm_rua_cliente="Rua ABC",
            num_casa_cliente="123",
            dt_nascimento_cliente="1996-09-01",
            dt_atualizacao="2026-01-28",
            tp_pessoa="PF",
            vl_renda="5000"
        )
    ]
)
def test_apply_silver_missing_column_raises_error(spark, data):

    df = spark.createDataFrame([data])

    with pytest.raises(Exception):
        SilverTransformations.apply(df)
