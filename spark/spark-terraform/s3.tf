# Create S3 bucket
resource "aws_s3_bucket" "s3_bucket" {
  bucket = "s3-5234624-emr-cluster"
}

# Set access control of bucket to private
resource "aws_s3_bucket_acl" "s3_emr_bucket_acl" {
  bucket = aws_s3_bucket.s3_bucket.id
  acl    = "private"
  depends_on = [aws_s3_bucket_ownership_controls.s3_bucket_acl_ownership]
}

# Resource to avoid error "AccessControlListNotSupported: The bucket does not allow ACLs"
resource "aws_s3_bucket_ownership_controls" "s3_bucket_acl_ownership" {
  bucket = aws_s3_bucket.s3_bucket.id
  rule {
    object_ownership = "ObjectWriter"
  }
}