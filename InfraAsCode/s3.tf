resource "aws_s3_bucket" "raw" {
  bucket = "bucket-${var.environment}-raw"
  
  # Only for development purposes
  force_destroy = true
}

resource "aws_s3_bucket" "bronze" {
  bucket = "bucket-${var.environment}-bronze"
  
  # Only for development purposes
  force_destroy = true
}

resource "aws_s3_bucket" "silver" {
  bucket = "bucket-${var.environment}-silver"

  # Only for development purposes
  force_destroy = true
}

resource "aws_s3_bucket" "gold" {
  bucket = "bucket-${var.environment}-gold"

  # Only for development purposes
  force_destroy = true
}

resource "aws_s3_object" "clientes_raw_csv" {
  bucket = aws_s3_bucket.raw.id
  key    = "clientes_sinteticos.csv"

  source = "${path.module}/../datasets/clientes_sinteticos.csv"

  content_type = "text/csv"

  etag = filemd5("${path.module}/../datasets/clientes_sinteticos.csv")
}

resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.raw.id
  key    = "scripts/etl_clientes.py"

  source = "${path.module}/../ETL/script.py"

  content_type = "text/x-python"

  etag = filemd5("${path.module}/../ETL/script.py")
}