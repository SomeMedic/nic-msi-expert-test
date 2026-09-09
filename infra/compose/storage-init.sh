#!/bin/sh
set -eu
# No tracing and no credential-bearing SDK/CLI output.
fail() { echo "Object storage initialization failed" >&2; exit 1; }
mc alias set local http://object-storage:9000 "$(cat /run/secrets/minio_root_user)" "$(cat /run/secrets/minio_root_password)" >/dev/null 2>&1 || fail
for bucket in originals artifacts debug; do
  mc mb --ignore-existing "local/$bucket" >/dev/null 2>&1 || fail
done
for service in backend ingest; do
  mc admin policy create local "expert-$service" "/policies/$service.json" >/dev/null 2>&1 || fail
  mc admin user add local "$(cat /run/secrets/${service}_s3_access_key)" "$(cat /run/secrets/${service}_s3_secret_key)" >/dev/null 2>&1 || fail
  mc admin policy attach local "expert-$service" --user "$(cat /run/secrets/${service}_s3_access_key)" >/dev/null 2>&1 || fail
done
echo 'Object storage buckets and service principals ready'
