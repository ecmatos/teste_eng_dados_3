/*----- ENVIRONMENT VARIABLES ----- */

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "teste_eng_dados"
}

variable "environment" {
  type    = string
  default = "dev"
}

/*----- RESOURCE VARIABLES ----- */

variable "glue_role_arn" {
  type        = string
  description = "ARN of the Glue IAM Role"
}

variable "raw_bucket_id" {
  type        = string
  description = "Id of the raw S3 bucket"
}

variable "raw_bucket_name" {
  type        = string
  description = "Name of the raw S3 bucket"
}