resource "aws_iam_role" "redshift-role" {
  name = "redshift-role"
  assume_role_policy = jsonencode(
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Action": "sts:AssumeRole",
          "Principal": {
            "Service": "redshift.amazonaws.com"
          },
          "Effect": "Allow",
          "Sid": ""
        }
      ]
    }
  )

 tags = {
    Name        = "redshift-role"
    Environment = "dev"
  }
}

resource "aws_iam_role_policy" "redshift-s3-full-access-policy" {
  name = "${var.app_environment}-redshift-role-s3-policy"
  role = aws_iam_role.redshift-role.id

policy = jsonencode(
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": "s3:*",
          "Resource": "*"
          }
      ]
    }
) 
}