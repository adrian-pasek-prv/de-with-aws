resource "aws_vpc" "emr_vpc" {
  cidr_block           = "168.31.0.0/16"
  enable_dns_hostnames = true

  tags = {
    name = "emr_vpc"
  }
}

resource "aws_subnet" "emr_subnet" {
  vpc_id     = aws_vpc.emr_vpc.id
  cidr_block = "168.31.0.0/20"

  tags = {
    name = "emr_subnet"
  }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.emr_vpc.id
}

resource "aws_route_table" "r" {
  vpc_id = aws_vpc.emr_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_main_route_table_association" "a" {
  vpc_id         = aws_vpc.emr_vpc.id
  route_table_id = aws_route_table.r.id
}