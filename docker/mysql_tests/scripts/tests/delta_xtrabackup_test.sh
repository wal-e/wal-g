#!/bin/sh
set -e -x

. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysqlfullxtrabackupbucket
export WALG_DELTA_MAX_STEPS=5

mysqld --initialize --init-file=/etc/mysql/init.sql

service mysql start

sysbench --table-size=10 prepare

sysbench --time=5 run

mysql -e 'FLUSH LOGS'

wal-g backup-push

sysbench --time=5 run

mysql -e 'FLUSH LOGS'

mysqldump sbtest > /tmp/dump_before_second_backup

wal-g backup-push

mysql_kill_and_clean_data

wal-g backup-fetch LATEST

chown -R mysql:mysql $MYSQLDATA

service mysql start || (cat /var/log/mysql/error.log && false)

mysql_set_gtid_purged

mysqldump sbtest > /tmp/dump_after_restore

diff /tmp/dump_before_second_backup /tmp/dump_after_restore
