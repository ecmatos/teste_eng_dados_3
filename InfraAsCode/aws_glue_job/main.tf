/* ----- GLUE JOBS - ETL CLIENTES ----- */

resource "aws_glue_job" "clientes_etl" {
  name     = "${var.project_name}_${var.environment}_etl_clientes"
  description = "ETL Job to process client data from raw to bronze and silver layers"
  role_arn = var.glue_role_arn
  glue_version = "5.0"
  max_retries = 0
  timeout = 5
  number_of_workers = 10
  worker_type       = "G.1X"
  execution_class = "STANDARD"
  
  command {
    script_location = "s3://${var.raw_bucket_name}/${aws_s3_object.glue_etl_script.key}"
    name            = "glueetl"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter" = "true"
    "--enable-metrics"                  = "true"
    "--enable-spark-ui" = "true"
    "--TempDir" = "s3://${var.raw_bucket_name}/spark_temp/"
    "--spark-event-logs-path" = "s3://${var.raw_bucket_name}/spark_logs/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.tags
}

/* ----- FILES ----- */

resource "aws_s3_object" "glue_etl_script" {
  bucket = var.raw_bucket_id
  key    = "scripts/etl_clientes.py"

  source = "${path.module}/../../ETL/script.py"

  content_type = "text/x-python"

  etag = filemd5("${path.module}/../../ETL/script.py")
}
