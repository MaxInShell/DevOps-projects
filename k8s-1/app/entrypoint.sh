#!/bin/sh
# Если Vault Agent отрендерил креды - подхватываем их. Если нет,
# работаем на переменных из Secret (этапы 1-6 до внедрения Vault).
if [ -f /vault/secrets/db.env ]; then
  . /vault/secrets/db.env
  echo "credentials loaded from vault agent"
fi
exec "$@"
