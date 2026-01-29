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

variable "bronze_bucket_name" {
  type = string
  description = "Bronze bucket name"
}

variable "silver_bucket_name" {
  type = string
  description = "Silver bucket name"
}