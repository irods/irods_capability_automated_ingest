# Changelog

## [Unreleased]

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

[Unreleased]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.0...HEAD
[v0.3.0]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.2...v0.3.0
[v0.2.2]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.1...v0.2.2
[v0.2.1]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/irods/irods_capability_automated_ingest/compare/11f9825df721a19dd25dad70aa94e5aa73d1d941...v0.1.0
