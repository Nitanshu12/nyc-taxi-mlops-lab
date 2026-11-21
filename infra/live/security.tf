# S0: security groups and rules. One group per host role plus RDS; host-to-host rules reference
# the source group, never a CIDR, so they follow the hosts if their IPs change.
#
# Decision point: who may reach the app (ingress scope)? It is var.ingress_cidrs (terraform.tfvars);
# record the choice in decisions/.
# No SSH and no port 22 anywhere: shell and UIs go through SSM Session Manager.
# Ports from contracts/runtime.yaml.

resource "aws_security_group" "app" {
  name        = "lab-app"
  description = "App host: Traefik on 80 from ingress_cidrs"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-app" }
}

resource "aws_security_group" "platform" {
  name        = "lab-platform"
  description = "Platform host: MLflow from the app host"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-platform" }
}

resource "aws_security_group" "llm" {
  name        = "lab-llm"
  description = "LLM host: chat and embeddings from the app and platform hosts"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-llm" }
}

resource "aws_security_group" "rds" {
  name        = "lab-rds"
  description = "Postgres from the app and platform hosts"
  vpc_id      = data.aws_vpc.default.id
  tags        = { Name = "lab-rds" }
}

# --- ingress ---------------------------------------------------------------------------------

# traefik: {host: app, port: 80}
resource "aws_vpc_security_group_ingress_rule" "app_http" {
  for_each          = toset(var.ingress_cidrs)
  security_group_id = aws_security_group.app.id
  description       = "Traefik"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  cidr_ipv4         = each.value
}

# mlflow: {host: platform, port: 5000}; app -> platform
resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_app" {
  security_group_id            = aws_security_group.platform.id
  description                  = "MLflow from the app host"
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
  referenced_security_group_id = aws_security_group.app.id
}

# app and platform are the clients of llm and rds
locals {
  backend_clients = {
    app      = aws_security_group.app.id
    platform = aws_security_group.platform.id
  }
}

resource "aws_vpc_security_group_ingress_rule" "llm" {
  for_each                     = local.backend_clients
  security_group_id            = aws_security_group.llm.id
  description                  = "llm-chat and llm-embed from the ${each.key} host"
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8081
  referenced_security_group_id = each.value
}

# Postgres; app, platform -> rds
resource "aws_vpc_security_group_ingress_rule" "rds" {
  for_each                     = local.backend_clients
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from the ${each.key} host"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = each.value
}

# --- egress ----------------------------------------------------------------------------------
# Hosts pull images (GHCR, Docker Hub), talk to GitHub (runner), SSM, S3 and Secrets Manager over
# the internet: there is no NAT gateway or VPC endpoint to narrow this to. RDS needs no egress.

resource "aws_vpc_security_group_egress_rule" "hosts" {
  for_each = {
    app      = aws_security_group.app.id
    platform = aws_security_group.platform.id
    llm      = aws_security_group.llm.id
  }
  security_group_id = each.value
  description       = "All outbound IPv4"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}
