#!/bin/sh
set -e -x

CONFIG_FILE="/tmp/configs/partial_restore_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}
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

run_backup_logged ${TMP_CONFIG} ${PGDATA}

wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --restore-only=to_restore

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


if [ "$(psql -p 6000 -t -c "select data from heap order by a;" -d to_skip -A)" != "$(printf '1|1\n2|2\n3|3\n4|4')" ]; then
  echo "Partial restore of heap table doesn't work 1"
  exit 1
if [ "$(psql -p 6000 -t -c "select a, b from ao order by a, b;" -d to_skip -A)" != "$(printf '1|1\n2|2\n3|3\n4|4')" ]; then
  echo "Partial restore of ao table doesn't work 1"
  exit 1
if [ "$(psql -p 6000 -t -c "select a, b from co order by a, b;" -d to_skip -A)" != "$(printf '1|1\n2|2\n3|3\n4|4')" ]; then
  echo "Partial restore of co table doesn't work 1"
  exit 1
echo "First database partial restore success!!!!!!"

stop_and_delete_cluster_dir
cleanup
# wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

# /usr/lib/postgresql/10/bin/initdb ${PGDATA}

# echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
# echo "archive_command = 'wal-g --config=${TMP_CONFIG} wal-push %p && echo \"WAL pushing: %p\"'" >> ${PGDATA}/postgresql.conf

# /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
# /tmp/scripts/wait_while_pg_not_ready.sh

# psql -c "CREATE DATABASE first" postgres
# psql -c "CREATE DATABASE second" postgres
# psql -c "CREATE TABLE tbl1 (data integer); INSERT INTO tbl1 VALUES (1), (2);" first
# psql -c "CREATE TABLE tbl2 (data integer); INSERT INTO tbl2 VALUES (3), (4);" second
# sleep 1

# wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

# psql -c "INSERT INTO tbl1 VALUES (5), (6);" first
# psql -c "INSERT INTO tbl2 VALUES (7), (8);" second
# psql -c "SELECT pg_switch_wal();" postgres
# sleep 10


# /tmp/scripts/drop_pg.sh
# wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST --restore-only=first
# echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

# /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
# /tmp/scripts/wait_while_pg_not_ready.sh

# if [ "$(psql -t -c "select data from tbl1;" -d first -A)" = "$(printf '1\n2\n5\n6')" ]; then
#   echo "First database partial restore success!!!!!!"
# else
#   echo "Partial restore doesn't work :("
#   exit 1
# fi

# if psql -t -c "select data from tbl2;" -d second -A 2>&1 | grep -q "is not a valid data directory"; then
#   echo "Skipped database raises error, as it should be!!!!!"
# else
#   echo "Skipped database responses unexpectedly"
#   exit 1
# fi