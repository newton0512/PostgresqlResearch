# PostgreSQL Research

Исследовательский проект для бенчмарка PostgreSQL (и доступа через Trino) с таблицей `bonus_registry` в разных вариантах: без индекса, с хэш-партиционированием (64 бакета), с индексом по `accounted_for_bs_profile_id` и с индексом и партиционированием. Бенчмарки: батчевая загрузка, чтение по ключу партиции и набор стандартных запросов. Опционально — тест K6 на конкурентную вставку одной записи.

## Цели

- **Стек**: Node.js, pnpm, TypeScript, Docker (PostgreSQL + Trino, только каталог postgres).
- **Данные**: каталог данных PostgreSQL на хосте **E:\PostgreSQL_data** (bind mount).
- **Режимы**: запуск всего только в **PostgreSQL** или через **Trino → PostgreSQL** (все запросы идут через Trino в ту же БД).
- **Варианты таблиц**: `bonus_registry_plain`, `bonus_registry_part`, `bonus_registry_idx`, `bonus_registry_idx_part` в схеме `bench`.
- **Сценарий**: создать выбранную таблицу → батчевая загрузка (до BATCH_SIZE за раунд, по умолчанию 100M) → лог времени записи → бенчмарк чтения → бенчмарк запросов → повторять, пока всего строк ≥ RECORD_MAX (по умолчанию 500M). Тест K6 на вставку одной записи запускается **отдельно** от этого сценария.
- **Возобновление**: полный цикл `bench:full` сохраняет состояние в `logs/bench-full-state.json`; при повторном запуске с теми же параметрами уже пройденные этапы пропускаются.

## Требования

- Node.js 18+, pnpm, Docker.
- Для постоянного хранения данных PostgreSQL: создать каталог **E:\PostgreSQL_data** (или задать `PG_DATA_PATH` в `.env`).

## Быстрый старт

```bash
# Копируем .env и при необходимости задаём PG_DATA_PATH
cp .env.example .env

# Установка
pnpm install

# Запуск контейнеров (PostgreSQL + Trino)
pnpm run compose:up

# Создать одну таблицу (например plain) и запустить полный цикл (для теста уменьшите BATCH_SIZE/RECORD_MAX в .env)
pnpm run setup:tables -- --table plain
pnpm run bench:full -- --table plain
```

## Docker

- **Запуск**: `pnpm run compose:up` — поднимает PostgreSQL (порт 5432) и Trino (порт 8080). Данные PostgreSQL хранятся в `E:\PostgreSQL_data` (или в `PG_DATA_PATH`).
- **Остановка**: `pnpm run compose:down`
- **Перезапуск**: `pnpm run compose:restart`
- **Сброс** (удаляются только тома Docker; каталог данных на хосте не трогается): `pnpm run compose:reset`

## Переменные окружения (.env)

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE | Подключение к PostgreSQL | localhost, 5432, postgres, postgres, appdb |
| TRINO_HOST, TRINO_PORT, TRINO_CATALOG, TRINO_SCHEMA, TRINO_USER | Trino (для BENCH_MODE=trino) | localhost, 8080, postgres, bench, trino |
| BENCH_MODE | `postgres` или `trino` | postgres |
| BATCH_SIZE | Сколько строк добавлять за один раунд загрузки | 100000000 |
| RECORD_MAX | Остановиться, когда всего строк ≥ этого значения | 500000000 |
| TABLE_VARIANT | Вариант таблицы по умолчанию | plain |
| API_PORT | Порт API-сервера (для K6) | 3000 |
| PG_DATA_PATH | Путь на хосте к данным PostgreSQL (Docker) | E:/PostgreSQL_data |

## Команды

| Команда | Описание |
|---------|----------|
| `pnpm run compose:up` | Запустить PostgreSQL и Trino |
| `pnpm run compose:down` | Остановить контейнеры |
| `pnpm run compose:restart` | Перезапустить контейнеры |
| `pnpm run setup:tables` | Создать таблицу(ы). Опции: `--table plain\|part\|idx\|idx_part` или `--all` |
| `pnpm run drop:tables` | Удалить таблицу(ы) в схеме bench. Опции: `--table plain\|part\|idx\|idx_part` или `--all` |
| `pnpm run bench:fill` | Батчевая загрузка (INSERT…SELECT в PG). Опции: `--table`, `--count N`, `--batch N` (размер батча в строках, по умолчанию 5M) |
| `pnpm run bench:read` | Бенчмарк чтения по `accounted_for_bs_profile_id`. Опции: `--table`, `--samples N` |
| `pnpm run bench:queries` | Бенчмарк стандартных запросов. Опции: `--table`, `--runs N` |
| `pnpm run bench:full` | Полный цикл: создать таблицу → загрузка → лог → бенчмарк чтения → бенчмарк запросов, повтор до RECORD_MAX. Опции: `--table`, `--batch N` (размер батча загрузки, по умолчанию 5M). **Возобновляемый**: состояние в `logs/bench-full-state.json` — при повторном запуске с теми же параметрами выполненные этапы пропускаются. Чтобы начать с нуля — удалите файл состояния или измените `--table`/`--batch`/BATCH_SIZE/RECORD_MAX. |
| `pnpm run api:server` | Запуск API для K6 (POST /api/insert-one) |
| `pnpm run k6:insert-one` | Запуск K6-теста конкурентной вставки одной записи (нужны K6 и API). Результаты в `k6-results/` |

**Важно**: K6 (insert-one) **не входит** в `bench:full`. Запускайте `api:server` и `k6:insert-one` отдельно, когда нужно.

### Возобновление bench:full

`bench:full` сохраняет прогресс в **logs/bench-full-state.json** (таблица, раунд, число строк, флаги этапов: create, fill, read, queries). При следующем запуске с теми же `--table`, `--batch`, BATCH_SIZE и RECORD_MAX уже выполненные этапы пропускаются. Чтобы начать цикл заново для тех же параметров, удалите этот файл. Смена таблицы, батча или лимитов в `.env` также приводит к новому прогону. Бенчмарки чтения и запросов выполняются в том же процессе, что и `bench:full`, чтобы гарантировать единое подключение к БД и корректный подсчёт строк.

## Куда пишутся результаты

- **logs/** — логи времени записи из `bench:fill` (например `write-plain-<timestamp>.log`), а также **logs/bench-full-state.json** — состояние полного цикла `bench:full` (таблица, раунд, количество строк, какие этапы уже выполнены). По этому файлу при повторном запуске пропускаются уже пройденные шаги.
- **results/** — результаты бенчмарков чтения и запросов (например `read-benchmark-plain-<timestamp>.txt`, `queries-benchmark-plain-<timestamp>.txt`).
- **k6-results/** — вывод K6 (например `insert-one-<timestamp>.json`) при запуске `k6:insert-one`.

## Два режима

1. **Только PostgreSQL** (`BENCH_MODE=postgres`): скрипты подключаются к PostgreSQL и выполняют DDL/DML напрямую.
2. **Trino → PostgreSQL** (`BENCH_MODE=trino`): скрипты подключаются к Trino; все запросы идут в каталог `postgres`, схему `bench`. Таблицы по-прежнему создаются в PostgreSQL (например через `setup:tables`); Trino только читает и пишет в них.

## Варианты таблиц

| Вариант | Описание |
|---------|----------|
| plain | Без индексов, без партиционирования |
| part | Хэш-партиционирование по `accounted_for_bs_profile_id` (64 бакета), без дополнительного индекса |
| idx | Индекс по `accounted_for_bs_profile_id`, без партиционирования |
| idx_part | Индекс и хэш-партиционирование (64 бакета) |

## Развёртывание на сервере (Terraform + Ansible)

Один сервер в Selectel VPC (flavor как data-server в samples-generation), внешний IP, доп. том для данных Postgres. Ansible монтирует том в `/data`, поднимает Postgres и Trino с «серверными» настройками Trino; API по умолчанию не запускается.

### Переменные перед развёртыванием

**1. Terraform (обязательно — через окружение, секреты не в файлы):**
```bash
export TF_VAR_selectel_domain="533343"
export TF_VAR_selectel_username="Newton"
export TF_VAR_selectel_password="***"
export TF_VAR_selectel_openstack_password="***"
```

**2. Terraform — остальное** можно задать в `terraform/terraform.tfvars` (скопировать из `terraform.tfvars.example`) или через `TF_VAR_*`:
- `region` — регион Selectel (например `ru-9`);
- `data_flavor_id` — flavor сервера (например `1019` — 16 vCPU / 64 GB, как data-server в samples-generation);
- при необходимости: `availability_zone`, `disk_type`, `data_boot_disk_size_gb`, `data_volume_size_gb`, `data_volume_disk_type`, `ssh_public_key_path`.

**3. Ansible (подключение по SSH):**
```bash
export SSH_PRIVATE_KEY=~/.ssh/id_rsa_terraform
```
Публичный ключ для Terraform задаётся в tfvars: `ssh_public_key_path = "~/.ssh/id_rsa_terraform.pub"`.

**4. Ansible (опционально, если запускаете не из корня проекта):**
```bash
export ANSIBLE_CONFIG=/путь/к/PostgresqlResearch/ansible/ansible.cfg
export ANSIBLE_GROUP_VARS=/путь/к/PostgresqlResearch/ansible/inventory/group_vars
```

**5. В `ansible/inventory/group_vars/all.yml`** при необходимости поправьте:
- `repo_url` — URL репозитория PostgresqlResearch;
- `repo_ref` — ветка или тег (например `main`);
- `ansible_ssh_private_key_file` — путь к ключу, если не `~/.ssh/id_rsa_terraform`.

**Пароль Postgres** (не храните в all.yml): задайте одним способом:
- **Файл (рекомендуется):** скопируйте `ansible/inventory/group_vars/all_secret.example.yml` в `all_secret.yml`, подставьте пароль. Файл `all_secret.yml` в `.gitignore`.
- **Переменная окружения:** перед запуском плейбука выполните `export POSTGRES_PASSWORD='ваш_пароль'` (или добавьте в `.env` и выполните `set -a && source .env && set +a`).

После `terraform apply` скрипт `./scripts/refresh-inventory.sh` подставит в `all.yml` актуальный `data_public_ip` из Terraform.

**SSH:** ключ в cloud-init прописан пользователю **deploy**, не root. Подключение:
`ssh -i ~/.ssh/id_rsa_terraform deploy@<data_server_public_ip>`. Ansible настроен на `ansible_user: deploy` и `become: true`.

---

1. Задайте переменные окружения (п. 1–4 выше).
2. `cd terraform && terraform init && terraform apply`
3. Из корня проекта: `./scripts/refresh-inventory.sh` — обновит `ansible/inventory/group_vars/all.yml` и выведет строку для K6.
4. `ansible-playbook -i ansible/inventory/hosts.yml ansible/playbooks/site.yml`
5. На сервере API для K6 запускается вручную: `cd /opt/PostgresqlResearch && pnpm run api:server` (или через systemd по желанию).

Для K6 с вашего ПК после шага 3: `export K6_API_URL=http://<IP>:3000` (IP выведет refresh-inventory.sh), затем `pnpm run k6:insert-one`.

## K6 insert-one (отдельно от основного сценария)

1. При необходимости задайте `BENCH_MODE` и вариант таблицы в `.env`.
2. Запустите API: `pnpm run api:server`
3. Запустите K6 (установите [k6](https://k6.io/docs/get-started/installation/) или используйте Docker):  
   `pnpm run k6:insert-one`  
   Или: `K6_API_URL=http://host:3000 K6_VUS=20 K6_DURATION=60s k6 run k6-scripts/insert-one.js`
4. Результаты записываются в `k6-results/insert-one-<timestamp>.json`.
