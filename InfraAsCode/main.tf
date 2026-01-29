/* ----- MODULES ----- */

module "datalake" {
  source      = "./aws_datalake"
  environment = var.environment
}

module "glue_data_catalog" {
  source             = "./aws_glue_data_catalog"
  environment        = var.environment
  bronze_bucket_name = module.datalake.bronze_bucket_name
  silver_bucket_name = module.datalake.silver_bucket_name
}

module "iam_glue" {
  source            = "./aws_iam_glue"
  project_name      = var.project_name
  environment       = var.environment
  raw_bucket_arn    = module.datalake.raw_bucket_arn
  bronze_bucket_arn = module.datalake.bronze_bucket_arn
  silver_bucket_arn = module.datalake.silver_bucket_arn
}

module "glue_job" {
  source          = "./aws_glue_job"
  project_name    = var.project_name
  environment     = var.environment
  glue_role_arn   = module.iam_glue.glue_role_arn
  raw_bucket_id   = module.datalake.raw_bucket_id
  raw_bucket_name = module.datalake.raw_bucket_name
}
