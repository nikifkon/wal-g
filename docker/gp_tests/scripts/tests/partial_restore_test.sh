#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/partial_restore_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/pg_scripts/wrap_config_file.sh  ${TMP_CONFIG}
source /tmp/tests/test_functions/util.sh

bootstrap_gp_cluster
sleep 3
enable_pitr_extension
setup_wal_archiving

# insert_data
psql -p 6000 -c "DROP DATABASE IF EXISTS to_restore"
psql -p 6000 -c "CREATE DATABASE to_restore"
psql -p 6000 -d to_restore -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,2) AS a;"
psql -p 6000 -d to_restore -c "CREATE TABLE ao(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
psql -p 6000 -d to_restore -c "CREATE TABLE co(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
psql -p 6000 -d to_restore -c "INSERT INTO ao select i, i FROM generate_series(1,2)i;"
psql -p 6000 -d to_restore -c "INSERT INTO co select i, i FROM generate_series(1,2)i;"

psql -p 6000 -c "DROP DATABASE IF EXISTS to_skip"
psql -p 6000 -c "CREATE DATABASE to_skip"
psql -p 6000 -d to_skip -c "CREATE TABLE heap AS SELECT a FROM generate_series(1,2) AS a;"
psql -p 6000 -d to_skip -c "CREATE TABLE ao(a int, b int) WITH (appendoptimized = true) DISTRIBUTED BY (a);"
psql -p 6000 -d to_skip -c "CREATE TABLE co(a int, b int) WITH (appendoptimized = true, orientation = column) DISTRIBUTED BY (a);"
psql -p 6000 -d to_skip -c "INSERT INTO ao select i, i FROM generate_series(1,2)i;"
psql -p 6000 -d to_skip -c "INSERT INTO co select i, i FROM generate_series(1,2)i;"

run_backup_logged ${TMP_CONFIG} ${PGDATA}

psql -p 6000 -d to_restore -c "INSERT INTO heap select i FROM generate_series(3,4)i;"
psql -p 6000 -d to_restore -c "INSERT INTO ao select i, i FROM generate_series(3,4)i;"
psql -p 6000 -d to_restore -c "INSERT INTO co select i, i FROM generate_series(3,4)i;"

psql -p 6000 -d to_skip -c "INSERT INTO heap select i FROM generate_series(3,4)i;"
psql -p 6000 -d to_skip -c "INSERT INTO ao select i, i FROM generate_series(3,4)i;"
psql -p 6000 -d to_skip -c "INSERT INTO co select i, i FROM generate_series(3,4)i;"

stop_and_delete_cluster_dir

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --in-place --restore-only=to_restore

start_cluster

if [ "$(psql -p 6000 -t -c "select a from heap order by a;" -d to_restore -A)" != "$(printf '1\n2')" ]; then
  echo "Partial restore of heap table doesn't work"
  exit 1
if [ "$(psql -p 6000 -t -c "select a, b from ao order by a, b;" -d to_restore -A)" != "$(printf '1|1\n2|2')" ]; then
  echo "Partial restore of ao table doesn't work"
  exit 1
if [ "$(psql -p 6000 -t -c "select a, b from co order by a, b;" -d to_restore -A)" != "$(printf '1|1\n2|2')" ]; then
  echo "Partial restore of co table doesn't work"
  exit 1
echo "First database partial restore success!!!!!!"

if [[ -z "$(psql -p 6000 -t -c "select a from heap order by a;" -d to_skip -A 2>&1 >/dev/null | grep -q "is not a valid data directory")" ]]; then
  echo "Partial restore of heap table doesn't work 1"
  exit 1
if [[ -z "$(psql -p 6000 -t -c "select a, b from ao order by a, b;" -d to_skip -A 2>&1 >/dev/null | grep -q "is not a valid data directory")" ]]; then
  echo "Partial restore of ao table doesn't work 1"
  exit 1
if [[ -z "$(psql -p 6000 -t -c "select a, b from co order by a, b;" -d to_skip -A 2>&1 >/dev/null | grep -q "is not a valid data directory")" ]]; then
  echo "Partial restore of co table doesn't work 1"
  exit 1
echo "First database partial restore success!!!!!!"

cleanup
