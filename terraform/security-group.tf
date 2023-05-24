resource "aws_default_security_group" "redshift_security_group" {
  depends_on = [aws_vpc.redshift-vpc]
  
  vpc_id = aws_vpc.redshift-vpc.id
  
  ingress {
    description = "Redshift Port"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] 
  }
  
  tags = {
    Name        = "redshift-security-group"
    Environment = "dev"
  }
}