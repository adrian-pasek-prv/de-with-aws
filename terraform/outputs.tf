# These values will be exported to terraform_output.json

# Cluster enpoint needed to interact with Redshift in Notebook.
output "cluster_endpoint" {
  description = "The connection endpoint"
  value       = aws_redshift_cluster.redshift-cluster.endpoint
}

# Redshift IAM role ARN
output "redshift_iam_arn" {
  description = "Redshift IAM role ARN"
  value       = aws_iam_role.redshift-role.arn
}