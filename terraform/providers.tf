terraform {
  required_version = ">= 1.0"

  required_providers {
    selectel = {
      source  = "selectel/selectel"
      version = "~> 7.1.0"
    }
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 3.0"
    }
  }
}

provider "selectel" {
  domain_name = var.selectel_domain
  username    = var.selectel_username
  password    = var.selectel_password
  auth_region = var.region
  auth_url    = "https://cloud.api.selcloud.ru/identity/v3/"
}

provider "openstack" {
  auth_url    = "https://cloud.api.selcloud.ru/identity/v3"
  domain_name = var.selectel_domain
  tenant_id   = selectel_vpc_project_v2.project.id
  user_name   = selectel_iam_serviceuser_v1.openstack.name
  password    = var.selectel_openstack_password
  region      = var.region
}
