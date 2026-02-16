# Terraform: PostgresqlResearch (один сервер, Selectel VPC)

Один сервер с flavor как у data-server в samples-generation, внешний IP, дополнительный том для данных Postgres. Ansible монтирует том в `/data` и размещает там данные PostgreSQL.

## Переменные окружения (обязательно)

```bash
export TF_VAR_selectel_domain="533343"
export TF_VAR_selectel_username="Newton"
export TF_VAR_selectel_password="***"
export TF_VAR_selectel_openstack_password="***"
```

## tfvars

Скопируйте `terraform.tfvars.example` в `terraform.tfvars` и задайте `data_flavor_id`, размеры дисков, `region`, `availability_zone`. Секреты в tfvars не храните.

## Применение

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

## Outputs

- `data_server_public_ip` — публичный IP (для Ansible и K6_API_URL)
- `data_server_private_ip` — приватный IP
- `ansible_data_public_ip` — то же, что public (для inventory)

После `terraform apply` выполните `./scripts/refresh-inventory.sh` из корня проекта, чтобы обновить Ansible inventory и получить строку для K6.
