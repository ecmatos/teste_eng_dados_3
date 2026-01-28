"""
Unit tests for silver transformations.
"""

import pytest
from pyspark.sql import Row
from ETL.script import SilverTransformations


class TestSilverTransformation:


    # Happy path
    @pytest.mark.parametrize(
        "data, expected_schema", 
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
                {
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
            )
        ]
    )
    def test_transformation(self, spark, data, expected_schema):

        df = spark.createDataFrame([data])
        result = SilverTransformations.apply(df)

        row = result.collect()[0]
        actual_schema = dict(result.dtypes)

        assert row.cod_cliente == "1"
        assert row.num_telefone_cliente == "(11)91234-5678"
        assert row.dt_nascimento_cliente.year == 1996
        assert row.vl_renda == 5000.00

        for field, expected_type in expected_schema.items():
            assert result.schema[field].dataType.simpleString() == expected_type


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
    def test_deduplication_by_dt_atualizacao(self, spark, data, expected_phone, expected_income):

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
    def test_invalid_phone(self, spark, data, expected_phone):

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
    def test_missing_column_raises_error(self, spark, data):

        df = spark.createDataFrame([data])

        with pytest.raises(Exception):
            SilverTransformations.apply(df)
