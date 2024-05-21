# iRODS Automated Ingest Framework

The automated ingest framework gives iRODS an enterprise solution that solves two major use cases: getting existing data under management and ingesting incoming data hitting a landing zone.

Based on the Python iRODS Client and Celery, this framework can scale up to match the demands of data coming off instruments, satellites, or parallel filesystems.

The example diagrams below show a filesystem scanner and a landing zone.

![Automated Ingest: Filesystem Scanner Diagram](capability_automated_ingest_filesystem_scanner.jpg)

![Automated Ingest: Landing Zone Diagram](capability_automated_ingest_landing_zone.jpg)

## Usage options

### Redis options
| option | effect | default |
| ----   |  ----- |  ----- |
| redis_host | Domain or IP address of Redis host | localhost |
| redis_port | Port number for Redis | 6379 |
| redis_db | Redis DB number to use for ingest | 0 |

### S3 options
To scan S3 bucket, minimally requires `--s3_keypair` and source path of the form `/bucket_name/path/to/root/folder`.

| option | effect | default |
| ----   |  ----- |  ----- |
| s3_keypair | path to S3 keypair file | None |
| s3_endpoint_domain | S3 endpoint domain | s3.amazonaws.com |
| s3_region_name | S3 region name | us-east-1 |
| s3_proxy_url | URL to proxy for S3 access | None |
| s3_insecure_connection | Do not use SSL when connecting to S3 endpoint | False |

### Logging/Profiling options
| option | effect | default |
| ----   |  ----- |  ----- |
| log_filename | Path to output file for logs | None |
| log_level | Minimum level of message to log | None |
| log_interval | Time interval with which to rollover ingest log file | None |
| log_when | Type/units of log_interval (see TimedRotatingFileHandler) | None |

`--profile` allows you to use vis to visualize a profile of Celery workers over time of ingest job.

| option | effect | default |
| ----   |  ----- |  ----- |
| profile_filename | Specify name of profile filename (JSON output) | None |
| profile_level | Minimum level of message to log for profiling | None |
| profile_interval | Time interval with which to rollover ingest profile file | None |
| profile_when | Type/units of profile_interval (see TimedRotatingFileHandler) | None |

### Ingest start options
These options are used at the "start" of an ingest job.

| option | effect | default |
| ----   |  ----- |  ----- |
| job_name | Reference name for ingest job | a generated uuid |
| interval | Restart interval (in seconds). If absent, will only sync once. | None |
| file_queue | Name for the file queue. | file |
| path_queue | Name for the path queue. | path |
| restart_queue | Name for the restart queue. | restart |
| event_handler | Path to event handler file | None (see "event_handler methods" below) |
| synchronous | Block until sync job is completed | False |
| progress | Show progress bar and task counts (must have --synchronous flag) | False |
| ignore_cache | Ignore last sync time in cache - like starting a new sync | False |

### Optimization options
| option | effect | default |
| ----   |  ----- |  ----- |
| exclude_file_type | types of files to exclude: regular, directory, character, block, socket, pipe, link | None |
| exclude_file_name | a list of space-separated python regular expressions defining the file names to exclude such as "(\S+)exclude" "(\S+)\.hidden" | None |
| exclude_directory_name | a list of space-separated python regular expressions defining the directory names to exclude such as "(\S+)exclude" "(\S+)\.hidden" | None |
| files_per_task | Number of paths to process in a given task on the queue | 50 |
| initial_ingest | Use this flag on initial ingest to avoid check for data object paths already in iRODS | False |
| irods_idle_disconnect_seconds | Seconds to hold open iRODS connection while idle | 60 |

## available `--event_handler` methods

| method |  effect  | default |
| ----   |   ----- |  ----- |
| pre_data_obj_create   |   user-defined python  |  none |
| post_data_obj_create   | user-defined python  |  none |
| pre_data_obj_modify     |   user-defined python   |  none |
| post_data_obj_modify     | user-defined python  |  none |
| pre_coll_create    |   user-defined python |  none |
| post_coll_create    |  user-defined python   |  none |
| pre_coll_modify    |   user-defined python |  none |
| post_coll_modify    |  user-defined python   |  none |
| character_map | user-defined python | none |
| as_user |   takes action as this iRODS user |  authenticated user |
| target_path | set mount path on the irods server which can be different from client mount path | client mount path |
| to_resource | defines  target resource request of operation |  as provided by client environment |
| operation | defines the mode of operation |  `Operation.REGISTER_SYNC` |
| max_retries | defines max number of retries on failure | 0 |
| timeout | defines seconds until job times out | 3600 |
| delay | defines seconds between retries | 0 |

Event handlers can use `logger` to write logs. See `structlog` for available logging methods and signatures.

### Operation mode

| operation |  new files  | updated files  |
| ----   |   ----- | ----- |
| `Operation.REGISTER_SYNC` (default)   |  registers in catalog | updates size in catalog |
| `Operation.REGISTER_AS_REPLICA_SYNC`  |   registers first or additional replica | updates size in catalog |
| `Operation.PUT`  |   copies file to target vault, and registers in catalog | no action |
| `Operation.PUT_SYNC`  |   copies file to target vault, and registers in catalog | copies entire file again, and updates catalog |
| `Operation.PUT_APPEND`  |   copies file to target vault, and registers in catalog | copies only appended part of file, and updates catalog |
| `Operation.NO_OP` | no action | no action

`--event_handler` usage examples can be found [in the examples directory](irods_capability_automated_ingest/examples).

### Character Mapping option
If an application should require that iRODS logical paths produced by the ingest process exclude subsets of the
range of possible Unicode characters, we can add a character\_map method that returns a dict object. For example:

```
    class event_handler(Core):
        @staticmethod
        def character_map():
            return {
                re.compile('[^a-zA-Z0-9]'):'_'
            }
        # ...
```
The returned dictionary, in this case, indicates that the ingest process should replace all non-alphanumeric (as
well as non-ASCII) characters with an underscore wherever they may occur in an otherwise normally generated logical path.
The substition process also applies to the intermediate (ie collection name) elements in a logical path, and a suffix is
appended to affected path elements to avoid potential collisions with other remapped object names.

Each key of the returned dictionary indicates a character or set of characters needing substitution.
Possible key types include:

   1. character
   ```
       # substitute backslashes with underscores
       '\\': '_'
   ```
   2. tuple of characters
   ```
       # any character of the tuple is replaced by a Unicode small script x
       ('\\','#','-'): '\u2093'
   ```
   3. regular expression
   ```
       # any character outside of range 0-256 becomes an underscore
       re.compile('[\u0100-\U0010ffff]'): '_'
   ```
   4. callable accepting a character argument and returning a boolean
   ```
       # ASCII codes above 'z' become ':'
       (lambda c: ord(c) in range(ord('z')+1,128)): ':'
   ```

In the event that the order-of-substitution is significant, the method may instead return a list of key-value tuples.

### UnicodeEncodeError
Any file whose path in the filesystem whose ingest results in a UnicodeEncodeError exception being raised (e.g. by the
inclusion of an unencodable UTF8 sequence) will be automatically renamed using a base-64 sequence to represent the original,
unmodified vault path.

Additionally, data objects that have had their names remapped, whether pro forma or via a UnicodeEncodeError, will be
annotated with an AVU of the form

   Attribute:	"irods::automated_ingest::" + ANNOTATION_REASON
   Value:	A PREFIX plus the base64-converted "bad filepath"
   Units:	"python3.base64.b64encode(full_path_of_source_file)"

Where :
   - ANNOTATION_REASON is either "UnicodeEncodeError" or "character\_map" depending on why the remapping occurred.
   - PREFIX is either "irods_UnicodeEncodeError\_" or blank(""), again depending on the re-mapping cause.

Note that the UnicodeEncodeError type of remapping is unconditional, whereas the character remapping is contingent on
an event handler's character_map method being defined.  Also, if a UnicodeEncodeError-style ingest is performed on a
given object, this precludes character mapping being done for the object.

## Manual Deployment

### Configure `python-irodsclient` environment

`python-irodsclient` (PRC) is used by the Automated Ingest tool to interact with iRODS. The configuration and client environment files used for a PRC application applies here as well.

If you are using PAM authentication, remember to use the [Client Settings File](https://github.com/irods/python-irodsclient/tree/71d787fe1f79d81775d892c59f3e9a9f60262c78?tab=readme-ov-file#python-irods-client-settings-file).

`iRODSSession`s are instantiated using an iRODS client environment file. The client environment file used can be controlled with the `IRODS_ENVIRONMENT_FILE` environment variable. If no such environment variable is set, the file is expected to be found at `${HOME}/.irods/irods_environment.json`. A secure connection can be made by making the appropriate configurations in the client environment file.

### Starting Redis Server
Install Redis server: [https://redis.io/docs/latest/get-started](https://redis.io/docs/latest/get-started)

Starting the Redis server with package installation:
```
redis-server
```
Or, dameonized:
```
sudo service redis-server start
```
```
sudo systemctl start redis
```

The Redis GitHub page also describes how to build and run Redis: [https://github.com/redis/redis?tab=readme-ov-file#running-redis](https://github.com/redis/redis?tab=readme-ov-file#running-redis)

The [Redis documentation](https://redis.io/topics/admin) also recommends an additional step:
> Make sure to set the Linux kernel overcommit memory setting to 1. Add vm.overcommit_memory = 1 to /etc/sysctl.conf and then reboot or run the command sysctl vm.overcommit_memory=1 for this to take effect immediately.

This allows the Linux kernel to overcommit virtual memory even if this exceeds the physical memory on the host machine. See [kernel.org documentation](https://www.kernel.org/doc/Documentation/vm/overcommit-accounting) for more information.

**Note:** If running in a distributed environment, make sure Redis server accepts connections by editing the `bind` line in /etc/redis/redis.conf or /etc/redis.conf.

### Setting up virtual environment
You may need to upgrade pip:
```
pip install --upgrade pip
```

Install virtualenv:
```
pip install virtualenv
```

Create a virtualenv with python3:
```
virtualenv -p python3 rodssync
```

Activate virtual environment:
```
source rodssync/bin/activate
```

### Install this package
```
pip install irods_capability_automated_ingest
```

Set up environment for Celery:
```
export CELERY_BROKER_URL=redis://<redis host>:<redis port>/<redis db> # e.g. redis://127.0.0.1:6379/0
export PYTHONPATH=`pwd`
```

Start celery worker(s):
```
celery -A irods_capability_automated_ingest.sync_task worker -l error -Q restart,path,file -c <num workers> 
```
**Note:** Make sure queue names match those of the ingest job (default queue names shown here).

### Using the sync script

#### Start sync job
```
python -m irods_capability_automated_ingest.irods_sync start <source dir> <destination collection>
```

#### List jobs
```
python -m irods_capability_automated_ingest.irods_sync list
```

#### Stop jobs
```
python -m irods_capability_automated_ingest.irods_sync stop <job name>
```

#### Watch jobs (same as using `--progress`)
```
python -m irods_capability_automated_ingest.irods_sync watch <job name>
```

## Run tests
**Note:** The tests start and stop their own Celery workers, and they assume a clean Redis database.
```
python -m irods_capability_automated_ingest.test.test_irods_sync
```
See [docker/ingest-test/README.md](docker/ingest-test/README.md) for how to run tests with Docker Compose.
