variable "environment_name" {
  description = "Суффикс для имен ресурсов (например dev/test)"
  type        = string
  default     = "pg-research"
}

variable "region" {
  description = "Регион Selectel (например ru-9)"
  type        = string
}

variable "availability_zone" {
  description = "AZ в регионе (например ru-9a)"
  type        = string
  default     = "ru-9a"
}

variable "disk_type" {
  description = "Тип диска Selectel (например fast/universal/basic/basic_hdd)"
  type        = string
  default     = "fast"
}

variable "private_subnet_cidr" {
  description = "CIDR приватной подсети"
  type        = string
  default     = "192.168.199.0/24"
}

variable "data_flavor_id" {
  description = "Flavor ID для сервера (как data-server в samples-generation, например 1019)"
  type        = string
}

variable "data_boot_disk_size_gb" {
  description = "Размер загрузочного диска (GB)"
  type        = number
  default     = 50
}

variable "data_volume_size_gb" {
  description = "Размер доп. тома для данных Postgres (GB). Монтируется в /data."
  type        = number
  default     = 300
}

variable "data_volume_disk_type" {
  description = "Тип диска для data-тома (например fast)"
  type        = string
  default     = "fast"
}

variable "ssh_public_key_path" {
  description = "Путь к SSH public key (например ~/.ssh/id_rsa_terraform.pub)"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "selectel_domain" {
  description = "Номер аккаунта Selectel (domain_name)"
  type        = string
}

variable "selectel_username" {
  description = "Логин Selectel"
  type        = string
}

variable "selectel_password" {
  description = "Пароль Selectel"
  type        = string
  sensitive   = true
}

variable "selectel_openstack_password" {
  description = "Пароль для IAM service-user (OpenStack)"
  type        = string
  sensitive   = true
}
