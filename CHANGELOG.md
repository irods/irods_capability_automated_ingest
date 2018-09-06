# Changelog

## [Unreleased]

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

[Unreleased]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.1...HEAD
[v0.2.1]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/irods/irods_capability_automated_ingest/compare/11f9825df721a19dd25dad70aa94e5aa73d1d941...v0.1.0
