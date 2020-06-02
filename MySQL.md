## WAL-G for MySQL

**Interface of MySQL and MariaDB now is unstable**

You can use wal-g as a tool for encrypting, compressing MySQL backups and push/fetch them to/from storage without saving it on your filesystem.

Development
-----------
### Installing
To compile and build the binary for MySQL:

Optional:

- To build with libsodium, just set `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, just set `USE_LZO` environment variable.
```
go get github.com/wal-g/wal-g
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make mysql_build
```
Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.
```
export GOBIN=/usr/local/bin
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make mysql_install
```

Configuration
-------------

* `WALG_MYSQL_DATASOURCE_NAME`

To configure the connection string for MySQL. Required. Format ```user:password@host/dbname```

* `WALG_MYSQL_SSL_CA`

To use SSL, a path to file with certificates should be set to this variable.

* `WALG_STREAM_CREATE_COMMAND`

Command to create MySQL backup, should return backup as single stream to STDOUT. Requried.

* `WALG_STREAM_RESTORE_COMMAND`

Command to unpack MySQL backup, should take backup (created by `WALG_STREAM_CREATE_COMMAND`) 
to STDIN and unpack it to MySQL datadir. Required.

* `WALG_MYSQL_BACKUP_PREPARE_COMMAND`

Command to prepare MySQL backup after restoring. Optional. Needed for xtrabackup case.

* `WALG_DELTA_MAX_STEPS`

Delta-backup is the difference between previously taken backup and present state. `WALG_DELTA_MAX_STEPS` determines how many delta backups can be between full backups. Defaults to 0.
Restoration process will automatically fetch all necessary deltas and base backup and compose valid restored backup. Supported with xtrabackup only.

* `WALG_MYSQL_BINLOG_REPLAY_COMMAND`

Command to replay binlog on runing MySQL. Required for binlog-fetch command.

* `WALG_MYSQL_BINLOG_DST`

To place binlogs in the specified directory during binlog-fetch or binlog-replay


> **Operations with binlogs**: If you'd like to do binlog operations with wal-g don't forget to [activate the binary log](https://mariadb.com/kb/en/activating-the-binary-log/) by starting mysql/mariadb with [--log-bin](https://mariadb.com/kb/en/replication-and-binary-log-server-system-variables/#log_bin) and [--log-basename](https://mariadb.com/kb/en/mysqld-options/#-log-basename)=\[name\].


Usage
-----

WAL-G mysql extension currently supports these commands:

* ``backup-push``

Creates new backup and send it to storage. Runs `WALG_STREAM_CREATE_COMMAND` to create backup.

```
wal-g backup-push
```

* ``backup-list``

Lists currently available backups in storage

```
wal-g backup-list
```

* ``backup-fetch``

Fetches backup from storage and restores it to datadir.
Runs `WALG_STREAM_RESTORE_COMMAND` to restore backup.
User should specify the name of the backup to fetch.

```
wal-g backup-fetch example_backup
```

WAL-G can also fetch the latest backup using:

```
wal-g backup-fetch  LATEST
```

* ``binlog-push``

Sends (not yet archived) binlogs to storage. Typically run in CRON.

```
wal-g binlog-push
```

* ``binlog-fetch``

Fetches binlogs from storage and saves them to `WALG_MYSQL_BINLOG_DST` folder.
User should specify the name of the backup starting with which to fetch an binlog.
User may also specify time in  RFC3339 format until which should be fetched (used for PITR).
User have to replay binlogs manually in that case.

```
wal-g binlog-fetch --since "backupname"
```
or
```
wal-g binlog-fetch --since "backupname" --until "2006-01-02T15:04:05Z07:00"
```
or
```
wal-g binlog-fetch --since LATEST --until "2006-01-02T15:04:05Z07:00"
```

* ``binlog-replay``

Fetches binlogs from storage and passes them to `WALG_MYSQL_BINLOG_REPLAY_COMMAND` to replay on running MySQL server.
User should specify the name of the backup starting with which to fetch an binlog.
User may also specify time in  RFC3339 format until which should be fetched (used for PITR).
Binlogs are temporarily save in `WALG_MYSQL_BINLOG_DST` folder.
Replay command gets name of binlog to replay via environment variable `WALG_MYSQL_CURRENT_BINLOG` and stop-date via `WALG_MYSQL_BINLOG_END_TS`, which are set for each invocation.

```
wal-g binlog-replay --since "backupname"
```
or
```
wal-g binlog-replay --since "backupname" --until "2006-01-02T15:04:05Z07:00"
```
or
```
wal-g binlog-replay --since LATEST --until "2006-01-02T15:04:05Z07:00"
```

Typical configurations
-----

### MySQL - using with `xtrabackup`


It's recommended to use wal-g with xtrabackup tool in case of MySQL for creating lock-less backups.
Here's typical wal-g configuration for that case:
```
 WALG_MYSQL_DATASOURCE_NAME=user:pass@tcp(localhost:3306)/mysql                                                                                                                                      
 WALG_STREAM_CREATE_COMMAND="xtrabackup --backup --stream=xbstream --datadir=/var/lib/mysql"                                                                                                                               
 WALG_STREAM_RESTORE_COMMAND="xbstream -x -C /var/lib/mysql"                                                                                                                       
 WALG_MYSQL_BACKUP_PREPARE_COMMAND="xtrabackup --prepare --target-dir=/var/lib/mysql"                                                                                              
 WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
```

Restore procedure is a bit tricky:
* stop mysql
* clean a datadir (typically `/var/lib/mysql`)
* fetch and prepare desired backup using `wal-g backup-fetch "backup_name"`
* start mysql
* in case of you have replication and GTID enabled: set mysql GTID_PURGED variable to value from `/var/lib/mysql/xtrabackup_binlog_info`, using
```bash
gtids=$(tr -d '\n' < /var/lib/mysql/xtrabackup_binlog_info | awk '{print $3}')
mysql -e "RESET MASTER; SET @@GLOBAL.GTID_PURGED='$gtids';"
```
* for PITR, replay binlogs with
```
wal-g binlog-replay --since "backup_name" --until "2006-01-02T15:04:05Z07:00"
```

### MySQL - using with `mysqldump`


It's possible to use wal-g with standard mysqldump/mysql tools.
In that case MySQL mysql backup is a plain SQL script.
Here's typical wal-g configuration for that case:

```
 WALG_MYSQL_DATASOURCE_NAME=user:pass@localhost/mysql                                                                                                               
 WALG_STREAM_CREATE_COMMAND="mysqldump --all-databases --single-transaction --set-gtid-purged=ON"                                                                                                                               
 WALG_STREAM_RESTORE_COMMAND="mysql"
 WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
```

Restore procedure is straightforward:
* start mysql (it's recommended to create new mysql instance)
* fetch and apply desired backup using `wal-g backup-fetch "backup_name"`


### MariaDB - using with `mariabackup`

It's recommended to use wal-g with `mariabackup` tool in case of MariaDB for creating lock-less backups.
Here's typical wal-g configuration for that case:
```
 WALG_MYSQL_DATASOURCE_NAME=user:pass@tcp(localhost:3305)/mysql                                                                                                                                      
 WALG_STREAM_CREATE_COMMAND="mariabackup --backup --stream=xbstream --datadir=/var/lib/mysql"                                                                                                                               
 WALG_STREAM_RESTORE_COMMAND="mbstream -x -C /var/lib/mysql"                                                                                                                       
 WALG_MYSQL_BACKUP_PREPARE_COMMAND="mariabackup --prepare --target-dir=/var/lib/mysql"                                                                                              
 WALG_MYSQL_BINLOG_REPLAY_COMMAND='mysqlbinlog --stop-datetime="$WALG_MYSQL_BINLOG_END_TS" "$WALG_MYSQL_CURRENT_BINLOG" | mysql'
```

For the restore procedure you have to do similar things to [what the offical docs says about full backup and restore](https://mariadb.com/kb/en/full-backup-and-restore-with-mariabackup/):
* stop mariadb
* clean a datadir (typically `/var/lib/mysql`)
* fetch and prepare desired backup using `wal-g backup-fetch "backup_name"`
* after the previous step you might have to fix file permissions: `chown -R mysql:mysql /var/lib/mysql`
* in case of restoring for a replication slave you can follow the [official docs](https://mariadb.com/kb/en/setting-up-a-replication-slave-with-mariabackup/#gtids)
* for PITR, replay binlogs with
```
wal-g binlog-replay --since "backup_name" --until "2006-01-02T15:04:05Z07:00"
```
* start mariadb

### MariaDB - using with `mysqldump`

The procedure is same as in case of [MySQL. You can follow the instructions from the previous section.](#mysql---using-with-mysqldump)
