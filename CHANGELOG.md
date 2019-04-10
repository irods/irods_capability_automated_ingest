# Changelog

## [Unreleased]

## [v0.3.5] - 2019-04-10

This release adds support for non utf-8 filenames
and tests for code coverage.

- [#88] Limit Celery version
- [#63] make easier to test against a non-default zone
- [#63] Add more UnicodeEncodeError tests
- [#51] Add tests for event handler PEPs
- [#31] Handle invalid zone name in target coll
- [#31] Add test for invalid zone name
- [#76] Add max redis version and requirements.txt
- [#40] Handle UnicodeEncodeError filenames for PUT
- [#40] Add tests for non-encodeable filename
- [#78] Add documentation around VM overcommitting

## [v0.3.4] - 2018-11-15

- [#76] Pin redis version to 2.10.6

## [v0.3.3] - 2018-10-27

- [#75] Honor SSL parameters in irods_environment.json

## [v0.3.2] - 2018-09-25

- [#69] Don't follow symlinks to dirs

## [v0.3.1] - 2018-09-20

- [#49] Fix S3 syncing dir and registering folder

## [v0.3.0] - 2018-09-19

This release adds support for scanning S3 in addition to
locally mounted filesystems.  To improve performance, a
default Celery worker will now work on 50 files, rather than 1.

- [#49] Add support for scanning S3
- [#51] Fix policy points for syncing directories
- [#52] Remove list_dir option

## [v0.2.2] - 2018-09-10

- [#50] fixed invocation used for collection events

## [v0.2.1] - 2018-09-06

- [#45] check permission before enqueueing a file/dir
- [#46] add missing scandir dependency
- [#47] only call cancel if timer is instantiated

## [v0.2.0] - 2018-09-03

- Swap queueing technology to Celery from RedisQueue
- Handles non-utf8-encodeable filenames
- Allows filetype/filename/directory exclusions
- Adds performance profiler
- Adds a NO_OP operation

## [v0.1.0] - 2018-05-11

- Initial release
- Python3 required
- Includes five operations
- Includes logging
- Nascent support for Docker, Kubernetes, and Helm

[Unreleased]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.5...HEAD
[v0.3.5]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.4...v0.3.5
[v0.3.4]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.3...v0.3.4
[v0.3.3]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.2...v0.3.3
[v0.3.2]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.1...v0.3.2
[v0.3.1]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.0...v0.3.1
[v0.3.0]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.2...v0.3.0
[v0.2.2]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.1...v0.2.2
[v0.2.1]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/irods/irods_capability_automated_ingest/compare/11f9825df721a19dd25dad70aa94e5aa73d1d941...v0.1.0
