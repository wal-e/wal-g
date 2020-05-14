#!/bin/sh
set -e -x

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '\
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_FILE_PREFIX=file://localhost/tmp \
WALE_LOG_DESTINATION=stderr \
/usr/bin/timeout 600 wal-e wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

pgbench -i -s 20 postgres
pg_dumpall -f /tmp/dump1
pgbench -c 2 -T 100000000 -S &
sleep 1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_FILE_PREFIX=file://localhost/tmp \
WALE_LOG_DESTINATION=stderr \
wal-e backup-push ${PGDATA}

pkill -9 postgres

mkdir /tmp/conf_files
cp ${PGDATA}/postgresql.conf /tmp/conf_files
cp ${PGDATA}/pg_hba.conf /tmp/conf_files
cp ${PGDATA}/pg_ident.conf /tmp/conf_files

ls -l /tmp/conf_files/

rm -rf ${PGDATA}

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_FILE_PREFIX=file://localhost/tmp \
WALE_LOG_DESTINATION=stderr \
wal-g backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&&\
 AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_FILE_PREFIX=file://localhost/tmp \
WALE_LOG_DESTINATION=stderr \
/usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

cp /tmp/conf_files/postgresql.conf ${PGDATA}
cp /tmp/conf_files/pg_hba.conf ${PGDATA}
cp /tmp/conf_files/pg_ident.conf ${PGDATA}

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2
rm -rf /tmp/conf_files
/tmp/scripts/drop_pg.sh

echo "WAL-E compatible backup-fetch success!!!!!!"
