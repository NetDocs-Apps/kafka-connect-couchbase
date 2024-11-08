module "ndcb-worker-secrets-offset-topic" {
  source      = "github.com/NetDocs-Apps/tf-msk-connector-mod-iac?ref=v0.1//mod/msk-worker-configuration"
  name        = "ndcb-worker-secrets-offset-topic"
  properties  = var.worker_configurations["ndcb-worker-secrets-offset-topic"]
  description = "Worker configuration with offset secrets topic provided."
}

