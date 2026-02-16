data "openstack_images_image_v2" "ubuntu" {
  name        = "Ubuntu 22.04 LTS 64-bit"
  most_recent = true
  visibility  = "public"

  depends_on = [
    selectel_vpc_project_v2.project,
    selectel_iam_serviceuser_v1.openstack
  ]
}
