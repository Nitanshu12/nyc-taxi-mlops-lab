# S1: wire the Redshift cluster (infra/modules/redshift) and expose the two outputs that
# contracts/platform.yaml lists under terraform_outputs:
#   - redshift_cluster_id
#   - redshift_database
#
# The module creates the cluster, its subnet group, and an internal security group that has
# no ingress (clients use the Data API) and open egress (COPY and UNLOAD reach S3 endpoints).
# LabRole is attached as the default IAM role, so COPY and UNLOAD say `iam_role default`.

module "redshift" {
  source            = "../modules/redshift"
  subnet_ids        = data.aws_subnets.default.ids
  availability_zone = var.availability_zone
}

output "redshift_cluster_id" {
  description = "contracts/platform.yaml terraform_outputs: redshift_cluster_id"
  value       = module.redshift.cluster_id
}

output "redshift_database" {
  description = "contracts/platform.yaml terraform_outputs: redshift_database"
  value       = module.redshift.database
}
