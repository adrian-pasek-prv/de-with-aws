## Main Postgres instance
resource "aws_db_instance" "aws_rds_postgres" {
  identifier             = "aws-rds-postgres"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20
  max_allocated_storage  = 100
  backup_retention_period = 1
  engine                 = "postgres"
  engine_version         = "14.6"
  db_name                = "postgres"
  username               = "postgres"
  password               = var.postgres_password
  publicly_accessible    = true
  skip_final_snapshot    = true
  apply_immediately      = true
}