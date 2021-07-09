#!/bin/sh
test_copy_composer()
{
  TMP_CONFIG=$1
  /usr/lib/postgresql/10/bin/initdb ${PGDATA}

  echo "archive_mode = on" >> ${PGDATA}/postgresql.conf
  echo "archive_command = '/usr/bin/timeout 600 wal-g --config=${TMP_CONFIG} wal-push %p'" >> ${PGDATA}/postgresql.conf
  echo "archive_timeout = 600" >> ${PGDATA}/postgresql.conf

  /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

  /tmp/scripts/wait_while_pg_not_ready.sh

  wal-g --config=${TMP_CONFIG} delete everything FORCE --confirm

  pgbench -i -s 5 postgres
  pg_dumpall -f /tmp/dump1
  
  wal-g --config=${TMP_CONFIG} backup-push ${PGDATA}

  wal-g --config=${TMP_CONFIG} backup-push ${PGDATA} --copy-composer  
  
  /tmp/scripts/drop_pg.sh

  wal-g --config=${TMP_CONFIG} backup-fetch ${PGDATA} LATEST

  echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& wal-g --config=${TMP_CONFIG} wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

  /usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
  /tmp/scripts/wait_while_pg_not_ready.sh
  pg_dumpall -f /tmp/dump2

  diff /tmp/dump1 /tmp/dump2

  psql -f /tmp/scripts/amcheck.sql -v "ON_ERROR_STOP=1" postgres

  echo "Full backup with copy-composer success!!!!!!"
}
