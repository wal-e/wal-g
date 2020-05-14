#!/bin/sh
set -e -x

/usr/lib/postgresql/10/bin/initdb ${PGDATA}

echo "archive_mode = on" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_command = '\
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_COMPRESSION_METHOD=$WALG_COMPRESSION_METHOD \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
/usr/bin/timeout 600 wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

mkdir /tmp/spaces
mkdir /tmp/spaces/space
mkdir /tmp/spaces/space2
psql -c "create tablespace space location '/tmp/spaces/space';"
psql -c "create table cinemas (id integer, name text, location text) tablespace space;"
psql -c "insert into cinemas (id, name, location) values (1, 'Inseption', 'USA');"
psql -c "insert into cinemas (id, name, location) values (2, 'Taxi', 'France');"
psql -c "insert into cinemas (id, name, location) values (3, 'Spirited Away', 'Japan');"
psql -c "create tablespace space2 location '/tmp/spaces/space2';"
psql -c "create table series (id integer, name text) tablespace space2;"
psql -c "insert into series (id, name) values (1, 'Game of Thrones');"
psql -c "insert into series (id, name) values (2, 'Black mirror');"
psql -c "insert into series (id, name) values (3, 'Sherlock');"
psql -c "create table users (id integer, name text, password text);"
psql -c "insert into users (id, name, password) values(1, 'ismirn0ff', 'password');"
psql -c "insert into users (id, name, password) values(2, 'tinsane', 'qwerty');"
psql -c "insert into users (id, name, password) values(3, 'godjan', 'g0djan');"
psql -c "insert into users (id, name, password) values(4, 'x4m', 'borodin');"
pg_dumpall -f /tmp/dump1
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
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
wal-g backup-push ${PGDATA}
pkill -9 postgres
pwd
cd /tmp/basebackups_005
cp -r /tmp/spaces /tmp/spaces_backup

rm -rf /tmp/spaces/*
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
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
wal-g backup-fetch ${PGDATA} LATEST

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& \
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_COMPRESSION_METHOD=$WALG_COMPRESSION_METHOD \
WALG_DELTA_MAX_STEPS=6 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALG_FILE_PREFIX=file://localhost/tmp \
WALG_LOG_DESTINATION=stderr \
/usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2

diff /tmp/dump1 /tmp/dump2
diff -r /tmp/spaces_backup /tmp/spaces

../scripts/drop_pg.sh

echo "Tablespaces work!!!"
