# Changelog

## [v0.4.1] - 2023-03-26

This release fixes an exit code bug and adds a
character_map event handler method.

- [#188] eliminate exit call in check_event_handler
- [#40][#166] tests work for unicodeEncodeError and char_map put/register
- [#166] implement object path character remapping (with AVU hints)
- [#180] add .gitignore
- [#177] Fix wrong exit code with --synchronous option

## [v0.4.0] - 2022-02-24

This release abstracts the scanners, eases deployment
by putting the event handler in redis, provides better
SSL support, and now requires Python 3.7+.

- [#171] Un-skip tests with resolved issues
- [#167] Bump versions in setup.py and test image
- [#170] Fix tests to use event_handler files
- Bump celery from 4.2.1 to 5.2.2
- Bump urllib3 from 1.24.2 to 1.26.5
- Bump jinja2 from 2.10 to 2.11.3
- [#102] event_handler goes into redis
- [#159] add performance benchmark test harness
- [#147][#157] Allow running workers with env only
- [#156] modified test to use resc_hier string
- [#155] added helper for unicode errors and renamed variables
- [#110] Add several interfaces for refactor
- [irods/python-irodsclient#237] load certificate into ssl context
- fixed the parsing of the S3 region parameter
- Bump werkzeug from 0.14.1 to 0.15.3
- [#125] Add non-SSL connection option for S3
- [#86][#117] Test suite cleanup + docker image
- Correct README.md for docker instructions
- [#109] Update docker steps for Celery
- [#114] Remove zone hint check
- [#90] Honor CELERY_BROKER_URL when present

## [v0.3.8] - 2019-11-12

This release fixes handling of stopped periodic jobs

- [#103] revoke scheduled celery restart jobs on stop

## [v0.3.7] - 2019-08-27

This release fixes a prefix handling bug when scanning S3.

- [#98] Preserve trailing slash for S3 prefix

## [v0.3.6] - 2019-08-14

This release fixes a path registration bug when scanning
S3 and updates a dependency.

- Bump urllib3 from 1.24.1 to 1.24.2 
- [#95] Replaced lstrip with index and offset

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

[Unreleased]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.8...HEAD
[v0.3.8]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.7...v0.3.8
[v0.3.7]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.6...v0.3.7
[v0.3.6]: https://github.com/irods/irods_capability_automated_ingest/compare/v0.3.5...v0.3.6
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
