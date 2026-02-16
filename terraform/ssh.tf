resource "selectel_vpc_keypair_v2" "ssh_key" {
  name       = "pg-research-${var.environment_name}-key"
  public_key = file(var.ssh_public_key_path)
  user_id    = selectel_iam_serviceuser_v1.openstack.id
}
