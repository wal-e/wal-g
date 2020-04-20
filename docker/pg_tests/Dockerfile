FROM wal-g/docker_prefix:latest

COPY docker/pg_tests/scripts/ /

ARG walg_compression_method

ENV WALG_COMPRESSION_METHOD $walg_compression_method

CMD su postgres -c "/tmp/run_integration_tests.sh"