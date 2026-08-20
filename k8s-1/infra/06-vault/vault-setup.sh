#!/usr/bin/env bash
# Настройка Vault: Kubernetes auth + динамические креды к PostgreSQL.
# Запускать после того, как под vault-0 стал Running.
set -euo pipefail

NS=vault
APP_NS=shortener
PG_HOST=pg-cluster-rw.${APP_NS}.svc.cluster.local
PG_ADMIN_USER=postgres

# Пароль суперпользователя, который CNPG положил в свой секрет
PG_ADMIN_PASSWORD=$(kubectl -n "$APP_NS" get secret pg-cluster-superuser \
  -o jsonpath='{.data.password}' | base64 -d)

v() { kubectl -n "$NS" exec -i vault-0 -- env VAULT_TOKEN=root VAULT_ADDR=http://127.0.0.1:8200 vault "$@"; }

echo "==> включаю kubernetes auth"
v auth enable kubernetes 2>/dev/null || echo "уже включён"
v write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.default.svc:443"

echo "==> включаю database secrets engine"
v secrets enable database 2>/dev/null || echo "уже включён"

echo "==> подключаю PostgreSQL"
v write database/config/shortener-pg \
  plugin_name=postgresql-database-plugin \
  allowed_roles="shortener-role" \
  connection_url="postgresql://{{username}}:{{password}}@${PG_HOST}:5432/shortener?sslmode=disable" \
  username="${PG_ADMIN_USER}" \
  password="${PG_ADMIN_PASSWORD}"

echo "==> роль с TTL 1 час"
v write database/roles/shortener-role \
  db_name=shortener-pg \
  creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; \
    GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO \"{{name}}\"; \
    GRANT USAGE ON SCHEMA public TO \"{{name}}\";" \
  default_ttl="1h" \
  max_ttl="24h"

echo "==> политика и роль для подов"
v policy write shortener-policy - <<'POLICY'
path "database/creds/shortener-role" {
  capabilities = ["read"]
}
POLICY

v write auth/kubernetes/role/shortener \
  bound_service_account_names=shortener \
  bound_service_account_namespaces="${APP_NS}" \
  policies=shortener-policy \
  ttl=1h

echo
echo "Готово. Проверить выдачу кредов:"
echo "  kubectl -n $NS exec -it vault-0 -- env VAULT_TOKEN=root vault read database/creds/shortener-role"
