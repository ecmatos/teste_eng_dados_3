/* ----- IAM ROLE ----- */

resource "aws_iam_role" "glue_job_role" {
  name = "${var.project_name}_${var.environment}_glue_job_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    projeto = var.project_name
    environment = var.environment
  }
}

/* ----- IAM POLICY ----- */

resource "aws_iam_role_policy" "glue_job_policy" {
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.raw_bucket_arn,
          "${var.raw_bucket_arn}/*",
          var.bronze_bucket_arn,
          "${var.bronze_bucket_arn}/*",
          var.silver_bucket_arn,
          "${var.silver_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}
