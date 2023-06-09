#!/bin/bash
set -e -x

/home/gpadmin/run_greenplum.sh

pushd /tmp
set -x
tests/partial_restore_test.sh
set +x
# for i in tests/*.sh; do
#   echo
#   echo "===== RUNNING $i ====="
#   set -x
#   ./"$i";

#   set +x
#   echo "===== SUCCESS $i ====="
#   echo
# done
popd
