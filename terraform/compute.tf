resource "openstack_blockstorage_volume_v3" "data_boot" {
  name              = "pg-research-boot-${var.environment_name}"
  size              = var.data_boot_disk_size_gb
  image_id          = data.openstack_images_image_v2.ubuntu.id
  volume_type       = "${var.disk_type}.${var.availability_zone}"
  availability_zone = var.availability_zone
}

resource "openstack_blockstorage_volume_v3" "data_volume" {
  name              = "pg-research-data-${var.environment_name}"
  size              = var.data_volume_size_gb
  volume_type       = "${var.data_volume_disk_type}.${var.availability_zone}"
  availability_zone = var.availability_zone
}

resource "openstack_compute_instance_v2" "data" {
  name              = "pg-research-server-${var.environment_name}"
  flavor_id         = var.data_flavor_id
  key_pair          = selectel_vpc_keypair_v2.ssh_key.name
  availability_zone = var.availability_zone

  user_data = templatefile("${path.module}/cloud-init-bootstrap.yaml.tftpl", {
    ssh_public_key = file(var.ssh_public_key_path)
  })
  config_drive = true

  network {
    port = openstack_networking_port_v2.data.id
  }

  block_device {
    uuid                  = openstack_blockstorage_volume_v3.data_boot.id
    source_type           = "volume"
    destination_type      = "volume"
    boot_index            = 0
    delete_on_termination = true
  }

  block_device {
    uuid                  = openstack_blockstorage_volume_v3.data_volume.id
    source_type           = "volume"
    destination_type      = "volume"
    boot_index            = 1
    delete_on_termination = true
  }

  lifecycle {
    ignore_changes = [image_id]
  }

  vendor_options {
    ignore_resize_confirmation = true
  }

  depends_on = [openstack_networking_router_interface_v2.router_if]
}
