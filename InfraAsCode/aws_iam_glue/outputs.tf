output "glue_role_arn" {
  description = "ARN of the Glue IAM Role"
  value       = aws_iam_role.glue_job_role.arn
}