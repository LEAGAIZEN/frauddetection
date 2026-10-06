variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "model_bucket_name" {
  type    = string
  default = "fraud-model-artifacts"
}

variable "raw_data_bucket_name" {
  type    = string
  default = "fraud-raw-data"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "localstack_endpoint" {
  type      = string
  default   = "http://localhost:4566"
  sensitive = true
}