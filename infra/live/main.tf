# S0: the root module. One host per role in contracts/runtime.yaml, RDS, the artifact bucket and the
# lab/wiki secret container, all in the account's default VPC (public subnets, no NAT gateway: the
# hosts reach GHCR, GitHub and SSM through their public IPs; nothing is open to them except what
# security.tf allows).

data "aws_vpc" "default" {
  default = true
}

# The hosts' subnet: the default subnet in the chosen AZ.
data "aws_subnet" "hosts" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = var.availability_zone
  default_for_az    = true
}

# RDS needs a subnet group spanning at least two AZs: every default subnet.
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

module "platform" {
  source             = "../modules/host"
  role               = "platform"
  instance_type      = var.platform_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [aws_security_group.platform.id]
}

module "app" {
  source             = "../modules/host"
  role               = "app"
  instance_type      = var.app_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [aws_security_group.app.id]
}

module "llm" {
  source             = "../modules/host"
  role               = "llm"
  instance_type      = var.llm_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [aws_security_group.llm.id]
}

module "rds" {
  source             = "../modules/rds"
  instance_class     = var.rds_instance_class
  subnet_ids         = data.aws_subnets.default.ids
  security_group_ids = [aws_security_group.rds.id]
}

module "artifacts" {
  source = "../modules/bucket"
  name   = "lab-artifacts"
}

module "wiki_secret" {
  source      = "../modules/secret"
  name        = "lab/wiki" # contracts/runtime.yaml: secrets.wiki
  description = "Wiki URL and token (JSON: url, token). Value put out of band."
}

# S5: Grafana Cloud's remote-write URL, user and token for Alloy (JSON: url, user, token). Only the
# container is Terraform's; the value is put from your laptop so the token never sits in state or GitHub.
module "grafana_secret" {
  source      = "../modules/secret"
  name        = "lab/grafana"
  description = "Grafana Cloud remote-write URL, user and token (JSON: url, user, token). Value put out of band."
}
