#!/usr/bin/env bash

# common wal-g settings
export WALG_MYSQL_DATASOURCE_NAME=sbtest:@/sbtest
export WALG_STREAM_CREATE_COMMAND="xtrabackup --backup \
    --stream=xbstream \
    --user=sbtest \
    --host=localhost \
    --parallel=2 \
    --datadir=${MYSQLDATA}"
export WALG_STREAM_RESTORE_COMMAND="xbstream -x -C ${MYSQLDATA}"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND="xtrabackup --prepare --target-dir=${MYSQLDATA}"
export WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
export WALG_MYSQL_BINLOG_DST=/tmp
export WALG_DELTA_MAX_STEPS=0

# test tools
mysql_kill_and_clean_data() {
    kill -9 `pidof mysqld` || true
    rm -rf "${MYSQLDATA}"/*
    rm -rf /root/.walg_mysql_binlogs_cache
}

mysql_set_gtid_purged() {
    gtids=$(tr -d '\n' < /var/lib/mysql/xtrabackup_binlog_info | awk '{print $3}')
    echo "Gtids from backup $gtids"
    mysql -e "RESET MASTER; SET @@GLOBAL.GTID_PURGED='$gtids';"
}

sysbench() {
    /usr/bin/sysbench --verbosity=0 /usr/share/sysbench/oltp_insert.lua $@
}

date3339() {
    date --rfc-3339=ns | sed 's/ /T/'
}
