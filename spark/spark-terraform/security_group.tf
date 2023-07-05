
resource "aws_security_group" "emr_security_group" {
  name        = "emr_security_group"
  description = "Allow inbound traffic"
  vpc_id      = aws_vpc.emr_vpc.id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.emr_vpc.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  depends_on = [aws_subnet.emr_subnet]

  lifecycle {
    ignore_changes = [
      ingress,
      egress,
    ]
  }

  tags = {
    name = "emr_security_group"
  }
}
