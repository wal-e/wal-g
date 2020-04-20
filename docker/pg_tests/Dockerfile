FROM wal-g/docker_prefix:latest

COPY docker/pg_tests/scripts/ /

CMD su postgres -c "/tmp/run_integration_tests.sh"