output "data_server_public_ip" {
  value       = openstack_networking_floatingip_v2.data.address
  description = "Публичный IP сервера (для Ansible и K6)"
}

output "data_server_private_ip" {
  value       = openstack_compute_instance_v2.data.access_ip_v4
  description = "Приватный IP сервера"
}

output "ansible_data_public_ip" {
  value       = openstack_networking_floatingip_v2.data.address
  description = "Ansible: ansible_host (публичный IP)"
}

output "project_id" {
  value       = selectel_vpc_project_v2.project.id
  description = "Selectel VPC Project ID"
}
