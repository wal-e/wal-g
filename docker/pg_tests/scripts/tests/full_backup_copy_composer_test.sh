#!/bin/sh
set -e -x
CONFIG_FILE="/tmp/configs/full_backup_copy_composer_test_config.json"
COMMON_CONFIG="/tmp/configs/common_config.json"
TMP_CONFIG="/tmp/configs/tmp_config.json"
cat ${CONFIG_FILE} > ${TMP_CONFIG}
echo "," >> ${TMP_CONFIG}
cat ${COMMON_CONFIG} >> ${TMP_CONFIG}
/tmp/scripts/wrap_config_file.sh ${TMP_CONFIG}

. /tmp/tests/test_functions/test_copy_composer.sh
test_copy_composer ${TMP_CONFIG}