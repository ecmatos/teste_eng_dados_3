resource "aws_glue_catalog_database" "bronze" {
  name = "${var.environment}_datalake_bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${var.environment}_datalake_silver"
}

resource "aws_glue_catalog_table" "bronze_clientes" {
  name          = "tabela_cliente_landing"
  database_name = aws_glue_catalog_database.bronze.name
  
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.bronze.bucket}/tabela_cliente_landing/"
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
    location      = "s3://${aws_s3_bucket.silver.bucket}/tb_cliente/"
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

resource "aws_glue_job" "clientes_etl" {
  name     = "${var.project_name}_${var.environment}_etl_clientes"
  description = "ETL Job to process client data from raw to bronze and silver layers"
  role_arn = aws_iam_role.glue_job_role.arn
  glue_version = "5.0"
  max_retries = 0
  timeout = 5
  number_of_workers = 10
  worker_type       = "G.1X"
  execution_class = "STANDARD"
  
  command {
    script_location = "s3://${aws_s3_bucket.raw.bucket}/${aws_s3_object.glue_etl_script.key}"
    name            = "glueetl"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--continuous-log-logGroup"          = "/aws-glue/jobs"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter" = "true"
    "--enable-metrics"                  = "true"
    "--enable-spark-ui" = "true"
    "--TempDir" = "${aws_s3_bucket.raw.bucket}/spark_temp/"
    "--spark-event-logs-path" = "${aws_s3_bucket.raw.bucket}/spark_logs/"
  }


  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    projeto     = var.project_name
    environment = var.environment
  }
}
