# S2: wire infra/modules/notebook (the SageMaker notebook) into this root module, let it reach MLflow
# on the platform host (port 5000, runtime.yaml), and output its name as notebook_name.
# Set github_repo in terraform.tfvars: the notebook clones your public repo.

module "notebook" {
  count     = var.github_repo != null ? 1 : 0
  source    = "../modules/notebook"
  subnet_id = data.aws_subnet.hosts.id
  repo_url  = "https://github.com/${var.github_repo}.git"

  env = {
    MLFLOW_TRACKING_URI = "http://${module.platform.private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.redshift.cluster_id
    REDSHIFT_DATABASE   = module.redshift.database
    REDSHIFT_DB_USER    = module.redshift.admin_user # explore.ipynb queries through the Data API as this user
    LAB_ARTIFACT_BUCKET = module.artifacts.name
  }
}

resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_notebook" {
  count                        = var.github_repo != null ? 1 : 0
  security_group_id            = aws_security_group.platform.id
  description                  = "MLflow from the SageMaker notebook"
  ip_protocol                  = "tcp"
  from_port                    = 5000
  to_port                      = 5000
  referenced_security_group_id = module.notebook[0].security_group_id
}

output "notebook_name" {
  value = try(module.notebook[0].name, null)
}
