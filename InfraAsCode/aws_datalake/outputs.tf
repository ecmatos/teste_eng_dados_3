output "raw_bucket_id" {
  description = "Id of the raw S3 bucket"
  value       = aws_s3_bucket.raw.id
}

output "raw_bucket_name" {
  description = "Name of the raw S3 bucket"
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the raw S3 bucket"
  value       = aws_s3_bucket.raw.arn
}

output "bronze_bucket_name" {
  description = "Name of the bronze S3 bucket"
  value       = aws_s3_bucket.bronze.bucket
}

output "bronze_bucket_arn" {
  description = "ARN of the bronze S3 bucket"
  value       = aws_s3_bucket.bronze.arn
}

output "silver_bucket_name" {
  description = "Name of the silver S3 bucket"
  value       = aws_s3_bucket.silver.bucket
}

output "silver_bucket_arn" {
  description = "ARN of the silver S3 bucket"
  value       = aws_s3_bucket.silver.arn
}