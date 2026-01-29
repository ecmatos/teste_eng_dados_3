/* ----- DATA LAKE S3 BUCKETS ----- */

resource "aws_s3_bucket" "raw" {
  bucket        = "${local.datalake_prefix}-raw"
  force_destroy = true # only for development purposes
}

resource "aws_s3_bucket" "bronze" {
  bucket        = "${local.datalake_prefix}-bronze"
  force_destroy = true # only for development purposes
}

resource "aws_s3_bucket" "silver" {
  bucket        = "${local.datalake_prefix}-silver"
  force_destroy = true # only for development purposes
}

/* ----- FILES ----- */

resource "aws_s3_object" "clientes_raw_csv" {
  bucket       = aws_s3_bucket.raw.id
  key          = "clientes_sinteticos.csv"
  source       = "${path.module}/../../datasets/clientes_sinteticos.csv"
  content_type = "text/csv"
  etag         = filemd5("${path.module}/../../datasets/clientes_sinteticos.csv")
}