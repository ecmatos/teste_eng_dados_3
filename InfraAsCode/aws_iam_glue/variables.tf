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

variable "raw_bucket_arn" {
  type = string
  description = "ARN of Raw Bucket"
}

variable "bronze_bucket_arn" {
  type = string
  description = "ARN of Bronze Bucket"
}

variable "silver_bucket_arn" {
  type = string
  description = "ARN of Silver Bucket"
}