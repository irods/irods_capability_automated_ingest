FROM ingest:latest

ENV TEST_CASE=${TEST_CASE}

ENTRYPOINT python -m unittest ${TEST_CASE:-irods_capability_automated_ingest.test.test_irods_sync}
