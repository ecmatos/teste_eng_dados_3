/* ----- DATA LAKE S3 BUCKETS ----- */

resource "aws_s3_bucket" "raw" {
  bucket = "${local.datalake_prefix}-raw"
  
  # Only for development purposes
  force_destroy = true
}

resource "aws_s3_bucket" "bronze" {
  bucket = "${local.datalake_prefix}-bronze"
  
  # Only for development purposes
  force_destroy = true
}

resource "aws_s3_bucket" "silver" {
  bucket = "${local.datalake_prefix}-silver"

  # Only for development purposes
  force_destroy = true
}

/* ----- FILES ----- */

resource "aws_s3_object" "clientes_raw_csv" {
  bucket = aws_s3_bucket.raw.id
  key    = "clientes_sinteticos.csv"

  source = "${path.module}/../../datasets/clientes_sinteticos.csv"

  content_type = "text/csv"

  etag = filemd5("${path.module}/../../datasets/clientes_sinteticos.csv")
}