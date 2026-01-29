/* ----- GLUE DATABASES ----- */

resource "aws_glue_catalog_database" "bronze" {
  name = "${var.environment}_datalake_bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${var.environment}_datalake_silver"
}

/* ----- GLUE TABLES ----- */

resource "aws_glue_catalog_table" "bronze_clientes" {
  name          = "tabela_cliente_landing"
  database_name = aws_glue_catalog_database.bronze.name
  
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bronze_bucket_name}/tabela_cliente_landing/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "cod_cliente"
      type = "string"
    }

    columns {
      name = "nm_cliente"
      type = "string"
    }

    columns {
      name = "nm_pais_cliente"
      type = "string"
    }

    columns {
      name = "nm_cidade_cliente"
      type = "string"
    }

    columns {
      name = "nm_rua_cliente"
      type = "string"
    }

    columns {
      name = "num_casa_cliente"
      type = "string"
    }

    columns {
      name = "num_telefone_cliente"
      type = "string"
    }

    columns {
      name = "dt_nascimento_cliente"
      type = "string"
    }

    columns {
      name = "dt_atualizacao"
      type = "string"
    }

    columns {
      name = "tp_pessoa"
      type = "string"
    }

    columns {
      name = "vl_renda"
      type = "string"
    }
  }

  partition_keys {
    name = "anomesdia"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "silver_clientes" {
  name          = "tb_cliente"
  database_name = aws_glue_catalog_database.silver.name
  
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.silver_bucket_name}/tb_cliente/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "cod_cliente"
      type = "string"
    }

    columns {
      name = "nm_cliente"
      type = "string"
    }

    columns {
      name = "nm_pais_cliente"
      type = "string"
    }

    columns {
      name = "nm_cidade_cliente"
      type = "string"
    }

    columns {
      name = "nm_rua_cliente"
      type = "string"
    }

    columns {
      name = "num_casa_cliente"
      type = "string"
    }

    columns {
      name = "num_telefone_cliente"
      type = "string"
    }

    columns {
    name = "dt_nascimento_cliente"
    type = "date"
    }

    columns {
    name = "dt_atualizacao"
    type = "timestamp"
    }

    columns {
      name = "tp_pessoa"
      type = "string"
    }

    columns {
    name = "vl_renda"
    type = "decimal(15,2)"
    }
  }

  partition_keys {
    name = "anomesdia"
    type = "string"
  }
}
