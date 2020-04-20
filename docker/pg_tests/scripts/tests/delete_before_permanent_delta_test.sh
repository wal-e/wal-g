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
WALG_DELTA_MAX_STEPS=3 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
/usr/bin/timeout 600 /usr/bin/wal-g wal-push %p'" >> /var/lib/postgresql/10/main/postgresql.conf
echo "archive_timeout = 600" >> /var/lib/postgresql/10/main/postgresql.conf

/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start

/tmp/scripts/wait_while_pg_not_ready.sh

#delete all backups of any
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=3 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g delete everything FORCE --confirm

# push permanent and impermanent delta backups
for i in 1 2 3 4
do
    pgbench -i -s 1 postgres &
    sleep 1
    if [ $i -eq 3 ]
    then
        AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=3 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-push --permanent ${PGDATA}
        pg_dumpall -f /tmp/dump1
    else
        AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=3 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-push ${PGDATA}
    fi
done

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=3 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list --detail

# delete backups by pushing a full backup and running `delete retain 1`
# this should only delete the last impermanent delta backup
pgbench -i -s 1 postgres &
sleep 1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-push ${PGDATA}

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list --detail

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g delete retain 1 --confirm

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list

# restore the backup and compare with previous state
/tmp/scripts/drop_pg.sh
first_backup_name=`\
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list | sed '2q;d' | cut -f 1 -d " "`

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-fetch ${PGDATA} $first_backup_name

echo "restore_command = 'echo \"WAL file restoration: %f, %p\"&& \
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_COMPRESSION_METHOD=$WALG_COMPRESSION_METHOD \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
/usr/bin/wal-g wal-fetch \"%f\" \"%p\"'" > ${PGDATA}/recovery.conf
/usr/lib/postgresql/10/bin/pg_ctl -D ${PGDATA} -w start
/tmp/scripts/wait_while_pg_not_ready.sh
pg_dumpall -f /tmp/dump2
diff /tmp/dump1 /tmp/dump2

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list --detail

# delete all backups after previous tests
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g delete everything FORCE --confirm

# make impermanent base backup
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-push ${PGDATA}

imperm_backup=`\
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list | egrep -o "[0-9A-F]{24}"`

# make permanent base backup
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-push --permanent ${PGDATA}

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list --detail

# check that nothing changed when permanent backups exist
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list > /tmp/dump1

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g delete everything --confirm || true

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list > /tmp/dump2
diff /tmp/dump1 /tmp/dump2

rm /tmp/dump2
touch /tmp/dump2

# delete all backups
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g delete everything FORCE --confirm

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
AWS_ENDPOINT=http://s3:9000 \
AWS_S3_FORCE_PATH_STYLE=true \
WALG_DELTA_MAX_STEPS=0 \
WALG_UPLOAD_CONCURRENCY=10 \
WALG_DISK_RATE_LIMIT=41943040 \
WALG_NETWORK_RATE_LIMIT=10485760 \
PGSSLMODE=allow \
PGDATABASE=postgres \
PGHOST=/var/run/postgresql \
WALE_S3_PREFIX=s3://deletebeforepermanentdeltabucket \
WALG_USE_WAL_DELTA=true \
wal-g backup-list 2> /tmp/2 1> /tmp/1

# check that stdout not include any backup
! cat /tmp/1 | egrep -o "[0-9A-F]{24}" > /tmp/dump1
diff /tmp/dump1 /tmp/dump2

# check that stderr not include any backup
# stderr shuld be "INFO: ... No backups found"
! cat /tmp/2 | egrep -o "[0-9A-F]{24}" > /tmp/dump1
diff /tmp/dump1 /tmp/dump2

/tmp/scripts/drop_pg.sh


echo "Delete before permanent delta success!!!!!!"
