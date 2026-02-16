resource "openstack_networking_port_v2" "data" {
  name       = "pg-research-port-${var.environment_name}"
  network_id = openstack_networking_network_v2.private_net.id

  fixed_ip {
    subnet_id = openstack_networking_subnet_v2.private_subnet.id
  }

  security_group_ids = [openstack_networking_secgroup_v2.data.id]

  depends_on = [openstack_networking_router_interface_v2.router_if]
}
