resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-${var.environment}-raw"
}

resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-${var.environment}-bronze"
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-${var.environment}-silver"
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-${var.environment}-gold"
}

resource "aws_s3_object" "clientes_raw_csv" {
  bucket = aws_s3_bucket.raw.id
  key    = "clientes_sinteticos.csv"

  source = "${path.module}/../datasets/clientes_sinteticos.csv"

  content_type = "text/csv"

  etag = filemd5("${path.module}/../datasets/clientes_sinteticos.csv")
}
