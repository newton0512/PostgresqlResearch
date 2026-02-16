#!/bin/bash
# Обновляет data_public_ip в ansible/inventory/group_vars/all.yml из Terraform output.
# Выводит строку для K6 (загрузка с вашего ПК).
# Запуск: из корня PostgresqlResearch  →  ./scripts/refresh-inventory.sh
#        или  ./scripts/refresh-inventory.sh [terraform_dir] [ansible_dir]

set -e
TF_DIR="${1:-terraform}"
ANSIBLE_DIR="${2:-ansible}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DATA_PUBLIC=$(terraform -chdir="$TF_DIR" output -raw data_server_public_ip 2>/dev/null || true)

if [ -z "$DATA_PUBLIC" ]; then
  echo "Error: run 'terraform apply' first. Could not get data_server_public_ip." >&2
  exit 1
fi

ALL="$ANSIBLE_DIR/inventory/group_vars/all.yml"
if [ ! -f "$ALL" ]; then
  echo "Error: $ALL not found." >&2
  exit 1
fi

sed -i.bak "s/^data_public_ip: .*/data_public_ip: \"${DATA_PUBLIC}\"/" "$ALL"
rm -f "${ALL}.bak"

echo "Updated data_public_ip in inventory/group_vars/all.yml"
echo ""
echo "To run K6 from your machine (after starting API on server):"
echo "  export K6_API_URL=http://${DATA_PUBLIC}:3000"
echo "  pnpm run k6:insert-one"
echo "  # or: K6_API_URL=http://${DATA_PUBLIC}:3000 k6 run k6-scripts/insert-one.js"
