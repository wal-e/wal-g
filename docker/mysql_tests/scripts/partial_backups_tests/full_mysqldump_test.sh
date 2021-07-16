#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlfullmysqldumpbucket
export WALG_STREAM_CREATE_COMMAND="mysqldump --all-databases --single-transaction"
export WALG_STREAM_RESTORE_COMMAND="mysql"
export WALG_MYSQL_USE_PARTIAL_BACKUP_PUSH="true"
export WALG_MYSQL_PARTIAL_BACKUP_FILE_SIZE="102400"
export WALG_MYSQL_PREFETCHED_FILES_COUNT="5"
export WALG_MYSQL_BACKUP_PREPARE_COMMAND=


mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

sysbench --table-size=10 prepare

sysbench --time=5 run

mysql -e 'FLUSH LOGS'

mysqldump sbtest > /tmp/dump_before_backup

wal-g backup-push


ps aux | grep mysqld
pidof mysqld

mysql_kill_and_clean_data

mysqld --initialize --init-file=/etc/mysql/init.sql || (cat /var/log/mysql/error.log && false)

service mysql start

wal-g backup-fetch LATEST

mysqldump sbtest > /tmp/dump_after_restore

diff /tmp/dump_before_backup /tmp/dump_after_restore