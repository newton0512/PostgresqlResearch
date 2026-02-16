resource "openstack_networking_floatingip_v2" "data" {
  pool = "external-network"

  depends_on = [
    selectel_vpc_project_v2.project,
    selectel_iam_serviceuser_v1.openstack
  ]
}

resource "openstack_networking_floatingip_associate_v2" "data" {
  port_id     = openstack_networking_port_v2.data.id
  floating_ip = openstack_networking_floatingip_v2.data.address

  depends_on = [openstack_networking_router_interface_v2.router_if]
}
