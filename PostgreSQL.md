## WAL-G for PostgreSQL

You can use wal-g as a tool for making encrypted, compressed PostgreSQL backups(full and incremental) and push/fetch them to/from storage without saving it on your filesystem.

If you prefer use Docker Image, you can directly test wal-g with this [playground](https://github.com/stephane-klein/playground-postgresql-walg).

Development
-----------
### Installing

Prepare on Ubuntu:
```
sudo apt-get install liblzo2-dev
```

Prepare on Mac OS:
```
# brew command is Homebrew for Mac OS
brew install cmake
export USE_LIBSODIUM="true" # since we're linking libsodium later
./link_brotli.sh
./link_libsodium.sh
make install_and_build_pg
```
> The compiled binary to run is in `main/pg/wal-g`

To compile and build the binary for Postgres:

Optional:

- To build with libsodium, just set `USE_LIBSODIUM` environment variable.
- To build with lzo decompressor, just set `USE_LZO` environment variable.
```
go get github.com/wal-g/wal-g
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make pg_build
```

Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.

```
export GOBIN=/usr/local/bin
cd $GOPATH/src/github.com/wal-g/wal-g
make install
make deps
make pg_install
```

Configuration
-------------
WAL-G uses [the usual PostgreSQL environment variables](https://www.postgresql.org/docs/current/static/libpq-envars.html) to configure its connection, especially including `PGHOST`, `PGPORT`, `PGUSER`, and `PGPASSWORD`/`PGPASSFILE`/`~/.pgpass`.

`PGHOST` can connect over a UNIX socket. This mode is preferred for localhost connections, set `PGHOST=/var/run/postgresql` to use it. WAL-G will connect over TCP if `PGHOST` is an IP address.

* `WALG_DISK_RATE_LIMIT`

To configure disk read rate limit during ```backup-push``` in bytes per second.

* `WALG_NETWORK_RATE_LIMIT`
To configure the network upload rate limit during ```backup-push``` in bytes per second.


Concurrency values can be configured using:

* `WALG_DOWNLOAD_CONCURRENCY`

To configure how many goroutines to use during ```backup-fetch``` and ```wal-fetch```, use `WALG_DOWNLOAD_CONCURRENCY`. By default, WAL-G uses the minimum of the number of files to extract and 10.

* `WALG_PREFETCH_DIR`

By default WAL prefetch is storing prefetched data in pg_wal directory. This ensures that WAL can be easily moved from prefetch location to actual WAL consumption directory. But it may have negative consequences if you use it with pg_rewind in PostgreSQL 13.
PostgreSQL 13 is able to invoke restore_command during pg_rewind. Prefetched WAL can generate false failure of pg_rewind. To avoid it you can either turn off prefetch during rewind (set WALG_DOWNLOAD_CONCURRENCY = 1) or place wal prefetch folder outside PGDATA. For details see [this pgsql-hackers thread](https://postgr.es/m/CAFh8B=kW8yY3yzA1=-w8BT90ejDoELhU+zho7F7k4J6D_6oPFA@mail.gmail.com).

* `WALG_UPLOAD_CONCURRENCY`

To configure how many concurrency streams to use during backup uploading, use `WALG_UPLOAD_CONCURRENCY`. By default, WAL-G uses 16 streams.

* `WALG_UPLOAD_DISK_CONCURRENCY`

To configure how many concurrency streams are reading disk during ```backup-push```. By default, WAL-G uses 1 stream.

* `TOTAL_BG_UPLOADED_LIMIT` (e.g. `1024`)
Overrides the default `number of WAL files to upload during one scan`. By default, at most 32 WAL files will be uploaded.

* `WALG_SENTINEL_USER_DATA`

This setting allows backup automation tools to add extra information to JSON sentinel file during ```backup-push```. This setting can be used e.g. to give user-defined names to backups.

* `WALG_PREVENT_WAL_OVERWRITE`

If this setting is specified, during ```wal-push``` WAL-G will check the existence of WAL before uploading it. If the different file is already archived under the same name, WAL-G will return the non-zero exit code to prevent PostgreSQL from removing WAL.

* `WALG_LIBSODIUM_KEY`

To configure encryption and decryption with libsodium. WAL-G uses an [algorithm](https://download.libsodium.org/doc/secret-key_cryptography/secretstream#algorithm) that only requires a secret key.

* `WALG_LIBSODIUM_KEY_PATH`

Similar to `WALG_LIBSODIUM_KEY`, but value is the path to the key on file system. The file content will be trimmed from whitespace characters.

* `WALG_GPG_KEY_ID`  (alternative form `WALE_GPG_KEY_ID`) ⚠️ **DEPRECATED**

To configure GPG key for encryption and decryption. By default, no encryption is used. Public keyring is cached in the file "/.walg_key_cache".

* `WALG_PGP_KEY`

To configure encryption and decryption with OpenPGP standard. You can join multiline key using `\n` symbols into one line (mostly used in case of daemontools and envdir).
Set *private key* value, when you need to execute ```wal-fetch``` or ```backup-fetch``` command.
Set *public key* value, when you need to execute ```wal-push``` or ```backup-push``` command.
Keep in mind that the *private key* also contains the *public key*.

* `WALG_PGP_KEY_PATH`

Similar to `WALG_PGP_KEY`, but value is the path to the key on file system.

* `WALG_PGP_KEY_PASSPHRASE`

If your *private key* is encrypted with a *passphrase*, you should set *passphrase* for decrypt.

* `WALG_DELTA_MAX_STEPS`

Delta-backup is the difference between previously taken backup and present state. `WALG_DELTA_MAX_STEPS` determines how many delta backups can be between full backups. Defaults to 0.
Restoration process will automatically fetch all necessary deltas and base backup and compose valid restored backup (you still need WALs after start of last backup to restore consistent cluster).
Delta computation is based on ModTime of file system and LSN number of pages in datafiles.

* `WALG_DELTA_ORIGIN`

To configure base for next delta backup (only if `WALG_DELTA_MAX_STEPS` is not exceeded). `WALG_DELTA_ORIGIN` can be LATEST (chaining increments), LATEST_FULL (for bases where volatile part is compact and chaining has no meaning - deltas overwrite each other). Defaults to LATEST.

* `WALG_TAR_SIZE_THRESHOLD`

To configure the size of one backup bundle (in bytes). Smaller size causes granularity and more optimal, faster recovering. It also increases the number of storage requests, so it can costs you much money. Default size is 1 GB (`1 << 30 - 1` bytes).

* `WALG_PG_WAL_SIZE`

To configure the wal segment size if different from the postgres default of 16 MB

* `WALG_UPLOAD_WAL_METADATA`

To upload metadata related to wal files. `WALG_UPLOAD_WAL_METADATA` can be INDIVIDUAL (generates metadata for all the wal logs) or BULK( generates metadata for set of wal files) 
Sample metadata file (000000020000000300000071.json)
```
{
    "000000020000000300000071": {
    "created_time": "2021-02-23T00:51:14.195209969Z",
    "date_fmt": "%Y-%m-%dT%H:%M:%S.%fZ"
    }
}
```
If the parameter value is NOMETADATA or not specified, it will fallback to default setting (no wal metadata generation)

Usage
-----

* ``backup-fetch``

When fetching base backups, the user should pass in the name of the backup and a path to a directory to extract to. If this directory does not exist, WAL-G will create it and any dependent subdirectories.

```
wal-g backup-fetch ~/extract/to/here example-backup
```

WAL-G can also fetch the latest backup using:

```
wal-g backup-fetch ~/extract/to/here LATEST
```

WAL-G can fetch the backup with specific UserData (stored in backup metadata) using the `--target-user-data` flag or `WALG_FETCH_TARGET_USER_DATA` variable:
```
wal-g backup-fetch /path --target-user-data "{ \"x\": [3], \"y\": 4 }"
```

#### Reverse delta unpack

Beta feature: WAL-G can unpack delta backups in reverse order to improve fetch efficiency.

[Reverse delta unpack benchmark results](benchmarks/reverse-delta-unpack-26-03-2020.md)
 
To activate this feature, do one of the following:

* set the `WALG_USE_REVERSE_UNPACK`environment variable
* add the --reverse-unpack flag
```
wal-g backup-fetch /path LATEST --reverse-unpack
```

#### Redundant archives skipping

With [reverse delta unpack](#reverse-delta-unpack) turned on, you also can turn on redundant archives skipping.
Since this feature involves both backup creation and restore process, in order to fully enable it you need to do two things:

1. Optional. Increases the chance of archive skipping, but may result in slower backup creation. [Enable rating tar ball composer](#rating-composer-mode) for `backup-push`.

2. Enable redundant backup archives skipping during backup-fetch. Do one of the following:
  
* set the `WALG_USE_REVERSE_UNPACK` and `WALG_SKIP_REDUNDANT_TARS` environment variables
* add the `--reverse-unpack` and `--skip-redundant-tars` flags

```  
wal-g backup-fetch /path LATEST --reverse-unpack --skip-redundant-tars
```

* ``backup-push``

When uploading backups to S3, the user should pass in the path containing the backup started by Postgres as in:

```
wal-g backup-push /backup/directory/path
```

If backup is pushed from replication slave, WAL-G will control timeline of the server. In case of promotion to master or timeline switch, backup will be uploaded but not finalized, WAL-G will exit with an error. In this case logs will contain information necessary to finalize the backup. You can use backuped data if you clearly understand entangled risks.

``backup-push`` can also be run with the ``--permanent`` flag, which will mark the backup as permanent and prevent it from being removed when running ``delete``.

#### Remote backup

WAL-G backup-push allows for two data streaming options:

1. Running directly on the database server as the postgres user, wal-g can read the database files from the filesystem. This option allows for high performance, and extra capabilities, like Delta backups.

For uploading backups to S3 in streaming option 1, the user should pass in the path containing the backup started by Postgres as in:

```
wal-g backup-push /backup/directory/path
```

2. Alternatively, WAL-G can stream the backup data through the postgres BASE_BACKUP protocol. This allows WAL-G to stream the backup data through the tcp layer, allows to run remote, and allows WAL-G to run as a separate linux user. WAL-G does require a database connection with replication privilleges. Do note that the BASE_BACKUP protocol does not allow for multithreaded streaming, and that Delta backup currently is not implemented.

To stream the backup data, leave out the data directory. And to set the hostname for the postgres server, you can use the environment variable PGHOST, or the WAL-G argument --pghost.

```
# Inline
PGHOST=srv1 wal-g backup-push

# Export
export PGHOST=srv1
wal-g backup-push

# Use commandline option
wal-g backup-push --pghost srv1
```

The remote backup option can also be used to:
* Run Postgres on mutiple hosts (streaming replication), and backup with WAL-G using multihost configuration: ``wal-g backup-push --pghost srv1,srv2``
* Run Postgres on a windows host and backup with WAL-G on a linux host: ``PGHOST=winsrv1 wal-g backup-push``
* Schedule WAL-G as a Kubernetes CronJob

#### Rating composer mode

In the rating composer mode, WAL-G places files with similar updates frequencies in the same tarballs during backup creation. This should increase the effectiveness of `backup-fetch` [redundant archives skipping](#redundant-archives-skipping). Be aware that although rating composer allows saving more data, it may result in slower backup creation compared to the default tarball composer.

To activate this feature, do one of the following:

* set the `WALG_USE_RATING_COMPOSER`environment variable
* add the --rating-composer flag

```
wal-g backup-push /path --rating-composer
```

#### Create delta from specific backup
When creating delta backup (`WALG_DELTA_MAX_STEPS` > 0), WAL-G uses the latest backup as the base by default. This behaviour can be changed via following flags:

* `--delta-from-name` flag or `WALG_DELTA_FROM_NAME` environment variable to choose the backup with specified name as the base for the delta backup

* `--delta-from-user-data` flag or `WALG_DELTA_FROM_USER_DATA` environment variable to choose the backup with specified user data as the base for the delta backup

Examples:
```
wal-g backup-push /path --delta-from-name base_000000010000000100000072_D_000000010000000100000063
wal-g backup-push /path --delta-from-user-data "{ \"x\": [3], \"y\": 4 }"
```

When using the above flags in combination with `WALG_DELTA_ORIGIN` setting, `WALG_DELTA_ORIGIN` logic applies to the specified backup. For example:
```
list of backups in storage:
base_000000010000000100000040  # full backup
base_000000010000000100000046_D_000000010000000100000040  # 1st delta
base_000000010000000100000061_D_000000010000000100000046  # 2nd delta
base_000000010000000100000070  # full backup

export WALG_DELTA_ORIGIN=LATEST_FULL
wal-g backup-push /path --delta-from-name base_000000010000000100000046_D_000000010000000100000040

wal-g logs:
INFO: Selecting the backup with name base_000000010000000100000046_D_000000010000000100000040 as the base for the current delta backup...
INFO: Delta will be made from full backup.
INFO: Delta backup from base_000000010000000100000040 with LSN 140000060.
```

* ``wal-fetch``

When fetching WAL archives from S3, the user should pass in the archive name and the name of the file to download to. This file should not exist as WAL-G will create it for you.

WAL-G will also prefetch WAL files ahead of asked WAL file. These files will be cached in `./.wal-g/prefetch` directory. Cache files older than recently asked WAL file will be deleted from the cache, to prevent cache bloat. If the file is requested with `wal-fetch` this will also remove it from cache, but trigger fulfilment of cache with new file.

```
wal-g wal-fetch example-archive new-file-name
```

* ``wal-push``

When uploading WAL archives to S3, the user should pass in the absolute path to where the archive is located.

```
wal-g wal-push /path/to/archive
```

* ``wal-show``

Show information about the WAL storage folder. `wal-show` shows all WAL segment timelines available in storage, displays the available backups for them, and checks them for missing segments.

* if there are no gaps (missing segments) in the range, final status is `OK`
* if there are some missing segments found, final status is `LOST_SEGMENTS`

```
wal-g wal-show
```

By default, `wal-show` shows available backups for each timeline. To turn it off, add the `--without-backups` flag.

By default, `wal-show` output is plaintext table. For detailed JSON output, add the `--detailed-json` flag.

* ``wal-verify``

Run series of checks to ensure that WAL segment storage is healthy. Available checks:

`integrity` - ensure that there is a consistent WAL segment history for the cluster so WAL-G can perform a PITR for the backup. Essentially, it checks that all of the WAL segments in the range `[oldest backup start segment, current cluster segment)` are available in storage. If no backups found, `[1, current cluster segment)` range will be scanned.

![SegmentStatusIllustration](resources/wal_verify_segment_statuses.png)

In `integrity` check output, there are four statuses of WAL segments:

* `FOUND` segments are present in WAL storage
* `MISSING_DELAYED` segments are not present in WAL storage, but probably Postgres did not try to archive them via `archive_command` yet
* `MISSING_UPLOADING` segments are the segments which are not present in WAL storage, but looks like that they are in the process of uploading to storage
* `MISSING_LOST` segments are not present in WAL storage and not `MISSING_UPLOADING` nor `MISSING_DELAYED`

`ProbablyUploading` segments range size is taken from `WALG_UPLOAD_CONCURRENCY` setting.

`ProbablyDelayed` segments range size is controlled via `WALG_INTEGRITY_MAX_DELAYED_WALS` setting.  

Output consists of:
1. Status of `integrity` check:
    * `OK` if there are no missing segments 
    * `WARNING` if there are some missing segments, but they are not `MISSING_LOST` 
    * `FAILURE` if there are some `MISSING_LOST` segments
2. A list that shows WAL segments in chronological order grouped by timeline and status.

`timeline` - check if the current cluster timeline is greater than or equal to any of the storage WAL segments timelines. This check is useful to detect split-brain conflicts. Please note that this check works correctly only if new storage created, or the existing one cleaned when restoring from the backup or performing `pg_upgrade`.

Output consists of:
1. Status of `timeline` check:
    * `OK` if current timeline id matches the highest timeline id found in storage
    * `WARNING` if could not determine if current timeline matches the highest in storage
    * `FAILURE` if current timeline id is not equal to the highest timeline id found in storage
2. Current timeline id.
3. The highest timeline id found in WAL storage folder.

Usage:
``` bash
wal-g wal-verify [space separated list of checks]
# For example:
wal-g wal-verify integrity timeline # perform integrity and timeline checks
wal-g wal-verify integrity # perform only integrity check
```

By default, `wal-verify` output is plaintext. To enable JSON output, add the `--json` flag.

Example of the plaintext output:
```
[wal-verify] integrity check status: OK
[wal-verify] integrity check details:
+-----+--------------------------+--------------------------+----------------+--------+
| TLI | START                    | END                      | SEGMENTS COUNT | STATUS |
+-----+--------------------------+--------------------------+----------------+--------+
|   3 | 00000003000000030000004D | 0000000300000004000000F0 |            420 |  FOUND |
|   4 | 0000000400000004000000F1 | 000000040000000800000034 |            836 |  FOUND |
+-----+--------------------------+--------------------------+----------------+--------+
[wal-verify] timeline check status: OK
[wal-verify] timeline check details:
Highest timeline found in storage: 4
Current cluster timeline: 4
```

Example of the JSON output:
```
{
   "integrity":{
      "status":"OK",
      "details":[
         {
            "timeline_id":3,
            "start_segment":"00000003000000030000004D",
            "end_segment":"0000000300000004000000F0",
            "segments_count":420,
            "status":"FOUND"
         },
         {
            "timeline_id":4,
            "start_segment":"0000000400000004000000F1",
            "end_segment":"000000040000000800000034",
            "segments_count":836,
            "status":"FOUND"
         }
      ]
   },
   "timeline":{
      "status":"OK",
      "details":{
         "current_timeline_id":4,
         "highest_storage_timeline_id":4
      }
   }
}
```

* ``wal-receive``

Set environment variabe WALG_SLOTNAME to define the slot to be used (defaults to walg). The slot name can only consist of the following characters: [0-9A-Za-z_].
When uploading WAL archives to S3, the user should pass in the absolute path to where the archive is located.

```
wal-g wal-receive
```


* ``backup-mark``

Backups can be marked as permanent to prevent them from being removed when running ``delete``. Backup permanence can be altered via this command by passing in the name of the backup (retrievable via `wal-g backup-list --pretty --detail --json`), which will mark the named backup and all previous related backups as permanent. The reverse is also possible by providing the `-i` flag.

```
wal-g backup-mark example-backup -i
```


* ``catchup-push``

To create an catchup incremental backup, the user should pass the path to the master Postgres directory and the LSN of the replica
for which the backup is created.

Steps:
1) Stop replica
2) Get replica LSN (for example using pg_controldata command)
3) Start uploading incremental backup on master.

``` bash
wal-g catchup-push /path/to/master/postgres --from-lsn replica_lsn
```


* ``catchup-fetch``

To accept catchup incremental backup created by `catchup-push`, the user should pass the path to the replica Postgres
directory and name of the backup.

``` bash
wal-g catchup-fetch /path/to/replica/postgres backup_name
```


* ``copy``

This command will help to change the storage and move the set of backups there or write the backups on magnetic tape. For example, `wal-g copy --from=config_from.json --to=config_to.json` will copy all backups.

Flags:

- `-b, --backup-name string` Copy specific backup
- `-f, --from string` Storage config from where should copy backup
- `-t, --to string` Storage config to where should copy backup
- `-w, --without-history` Copy backup without history (wal files)
