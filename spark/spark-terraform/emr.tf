resource "aws_emr_cluster" "cluster" {
  name          = "emr-cluster"
  release_label = "emr-6.11.0"
  applications  = ["Spark", "Zeppelin"]

  ec2_attributes {
    subnet_id                         = aws_subnet.emr_subnet.id
    emr_managed_master_security_group = aws_security_group.emr_security_group.id
    emr_managed_slave_security_group  = aws_security_group.emr_security_group.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
  }

  master_instance_group {
    instance_type = "m5.xlarge"
  }

  core_instance_group {
    instance_count = 4
    instance_type  = "m5.xlarge"
  }

  tags = {
    name = "emr_cluster",
    enviroment = "dev"
  }

  configurations_json = <<EOF
  [
    {
      "Classification": "hadoop-env",
      "Configurations": [
        {
          "Classification": "export",
          "Properties": {
            "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
          }
        }
      ],
      "Properties": {}
    },
    {
      "Classification": "spark-env",
      "Configurations": [
        {
          "Classification": "export",
          "Properties": {
            "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
          }
        }
      ],
      "Properties": {}
    }
  ]
EOF

  service_role = aws_iam_role.iam_emr_service_role.arn

  log_uri = "s3://${aws_s3_bucket.s3_bucket.id}/elasticmapreduce/"
}
