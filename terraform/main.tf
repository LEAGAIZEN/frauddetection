terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3 = var.localstack_endpoint
  }
}

resource "aws_s3_bucket" "model_artifacts" {
  bucket = "${var.model_bucket_name}-${var.environment}"
}

resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.raw_data_bucket_name}-${var.environment}"
}

# Explicit dependency: this object can only be created AFTER the raw_data
# bucket exists. Terraform infers this automatically from the bucket
# reference below, but depends_on makes the ordering explicit and visible --
# this is the "multiple resources + dependency handling" concept flagged as
# still-open back in Phase 10.
resource "aws_s3_object" "raw_data_placeholder" {
  bucket     = aws_s3_bucket.raw_data.id
  key        = "creditcard/.keep"
  content    = "placeholder so the prefix exists before the producer ever writes here"
  depends_on = [aws_s3_bucket.raw_data]
}

output "model_bucket_name" {
  value = aws_s3_bucket.model_artifacts.bucket
}

output "raw_data_bucket_name" {
  value = aws_s3_bucket.raw_data.bucket
}