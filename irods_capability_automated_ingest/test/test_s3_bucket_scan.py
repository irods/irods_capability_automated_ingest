import unittest

import io
import os
import signal
import shutil
import subprocess
import tempfile
import textwrap
import time

from irods.data_object import irods_dirname, irods_basename
from irods.exception import CollectionDoesNotExist
from irods.meta import iRODSMeta
from irods.models import Collection, DataObject
from irods.session import iRODSSession

from irods_capability_automated_ingest.celery import app
from irods_capability_automated_ingest.redis_utils import get_redis
from irods_capability_automated_ingest.sync_job import sync_job
from irods_capability_automated_ingest.utils import DeleteMode, Operation
import irods_capability_automated_ingest.examples

from minio import Minio

from . import test_lib

# TODO(#286): Derive from the environment?
# This must be set as an environment variable in order for the Celery workers to communicate with the broker.
# Update this value if the hostname, port, or database for the Redis service needs to change.
os.environ["CELERY_BROKER_URL"] = "redis://redis:6379/0"


def start_workers(n=2, args=[]):
    if not args:
        args = ["-l", "info", "-Q", "restart,path,file"]
    workers = subprocess.Popen(
        [
            "celery",
            "-A",
            "irods_capability_automated_ingest",
            "worker",
            "-c",
            str(n),
        ]
        + args
    )
    return workers


def wait_for_job_to_finish(workers, job_name, timeout=60):
    r = get_redis(test_lib.get_redis_config())
    t0 = time.time()
    while timeout is None or time.time() - t0 < timeout:
        restart = r.llen("restart")
        i = app.control.inspect()
        act = i.active()
        if act is None:
            active = 0
        else:
            active = sum(map(len, act.values()))
        job_done = sync_job(job_name, r).done()
        if restart != 0 or active != 0 or not job_done:
            time.sleep(1)
        else:
            return
    # If we escape the loop, that means the job timed out.
    raise TimeoutError(
        f"Timed out after [{timeout}] seconds waiting for job [{job_name}] to complete."
    )


class test_s3_sync_operations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.restart_queue_name = "s3_sync_restart"
        cls.path_queue_name = "s3_sync_path"
        cls.file_queue_name = "s3_sync_file"
        test_lib.clear_redis()
        test_lib.irmtrash()
        cls.workers = start_workers(
            args=[
                "-l",
                "info",
                "-Q",
                f"{cls.restart_queue_name},{cls.path_queue_name},{cls.file_queue_name}",
            ]
        )
        cls.irods_session = iRODSSession(
            **test_lib.get_test_irods_client_environment_dict()
        )
        cls.job_name = "test_s3_sync_job"
        # TODO(#286): Derive this from the environment...
        cls.s3_endpoint_domain = "minio:19000"
        # TODO(#286): Derive these from the environment...
        cls.s3_access_key = "irods"
        cls.s3_secret_key = "irodsadmin"
        f = tempfile.NamedTemporaryFile("w+t", delete=False)
        # TODO(#264): This will not work on Windows...
        f.write(f"{cls.s3_access_key}\n{cls.s3_secret_key}")
        f.close()
        cls.s3_keypair_path = f.name
        # Establish a connection with Minio that persists for every test
        cls.minio_client = Minio(
            cls.s3_endpoint_domain,
            access_key=cls.s3_access_key,
            secret_key=cls.s3_secret_key,
            secure=False,
        )
        cls.bucket_name = "test-s3-put-sync-operation-bucket"
        cls.source_path = f"/{cls.bucket_name}"
        cls.minio_client.make_bucket(cls.bucket_name)
        cls.objects_list = {
            "/".join(["shallow_subfolder", "shallow_object.txt"]),
            "/".join(["deep_subfolder", "a", "b", "c", "object_c.txt"]),
            "/".join(["deep_subfolder", "x", "y", "z", "object_z.txt"]),
            "/".join(["top_level_object.txt"]),
        }

    @classmethod
    def tearDownClass(cls):
        test_lib.clear_redis()
        test_lib.irmtrash()
        cls.irods_session.cleanup()
        cls.workers.send_signal(signal.SIGINT)
        cls.workers.wait()
        cls.minio_client.remove_bucket(cls.bucket_name)

    def create_objects(self, objects_list):
        for obj in objects_list:
            # The prefix is everything between the bucket name and the "basename" of the object "path".
            self.minio_client.put_object(
                self.bucket_name, obj, data=io.BytesIO(obj.encode()), length=len(obj)
            )

    def setUp(self):
        self.create_objects(self.objects_list)
        # TODO(#280): The sync job fails without this sleep because the Celery workers don't yet see the S3 objects.
        time.sleep(1)
        self.destination_collection = "/".join(
            [
                "",
                self.irods_session.zone,
                "home",
                self.irods_session.username,
                "s3_sync_collection",
            ]
        )

    def tearDown(self):
        objects = list(self.minio_client.list_objects(self.bucket_name, recursive=True))
        for obj in objects:
            self.minio_client.remove_object(self.bucket_name, obj.object_name)
        test_lib.delete_collection_if_exists(
            self.destination_collection, recurse=True, force=True
        )

    @staticmethod
    def get_event_handler(operation):
        operation_strings = {
            Operation.NO_OP: "NO_OP",
            Operation.REGISTER_SYNC: "REGISTER_SYNC",
            Operation.REGISTER_AS_REPLICA_SYNC: "REGISTER_AS_REPLICA_SYNC",
            Operation.PUT: "PUT",
            Operation.PUT_SYNC: "PUT_SYNC",
            Operation.PUT_APPEND: "PUT_APPEND",
        }
        return textwrap.dedent(
            f"""
            from irods_capability_automated_ingest.core import Core
            from irods_capability_automated_ingest.utils import DeleteMode, Operation
            class event_handler(Core):
                @staticmethod
                def operation(session, meta, **options):
                    return Operation.{operation_strings[operation]}
            """
        )

    def run_sync(
        self,
        source_path,
        destination_collection,
        event_handler_path,
        job_name=None,
        ignore_cache=False,
        files_per_task=1,
        log_level=None,
        queue_names=tuple(),
        expected_failure_count=None,
    ):
        sync_script = "irods_capability_automated_ingest.irods_sync"
        # Construct an invocation of the sync script with various options.
        command = [
            "python",
            "-m",
            sync_script,
            "start",
            source_path,
            destination_collection,
            "--event_handler",
            event_handler_path,
            "--files_per_task",
            str(files_per_task),
            "--s3_keypair",
            self.s3_keypair_path,
            "--s3_endpoint_domain",
            self.s3_endpoint_domain,
            "--s3_insecure_connection",
        ]
        if ignore_cache:
            command.append("--ignore_cache")
        if log_level:
            command.extend(["--log_level", log_level])
        # The test workers watch non-default queue names so that no other Celery workers which happen to be watching
        # the same Redis database will pick up the work.
        if not queue_names:
            queue_names = tuple(
                [self.restart_queue_name, self.path_queue_name, self.file_queue_name]
            )
        command.extend(["--restart_queue", queue_names[0]])
        command.extend(["--path_queue", queue_names[1]])
        command.extend(["--file_queue", queue_names[2]])
        # job_name is required so that we can track the sync job and its failed tasks even after it has completed.
        if not job_name:
            job_name = self.job_name
        command.extend(["--job_name", job_name])
        # Now, schedule the job...
        proc = subprocess.Popen(command)
        proc.wait()
        # ...and then wait for the workers to complete the tasks.
        try:
            wait_for_job_to_finish(self.workers, job_name)
        except TimeoutError as e:
            self.fail(e)
        # Assert that the expected number of failed tasks for this job are found. A value of None means no tasks
        # failed for this job.
        self.assertEqual(
            sync_job(job_name, get_redis(test_lib.get_redis_config()))
            .failures_handle()
            .get_value(),
            expected_failure_count,
        )

    def assert_ingested_contents_exist_in_irods(self):
        for obj in self.objects_list:
            self.assertTrue(
                self.irods_session.data_objects.exists(
                    "/".join([self.destination_collection, obj])
                )
            )

    def test_s3_with_put(self):
        operation = Operation.PUT
        new_object_name = "test_s3_with_put"
        event_handler_contents = test_s3_sync_operations.get_event_handler(operation)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was ingested properly.
            self.run_sync(
                self.source_path, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            try:
                self.minio_client.put_object(
                    self.bucket_name,
                    new_object_name,
                    data=io.BytesIO(new_object_name.encode()),
                    length=len(new_object_name),
                )
                self.run_sync(
                    self.source_path, self.destination_collection, event_handler_path
                )
                self.assert_ingested_contents_exist_in_irods()
                self.assertTrue(
                    self.irods_session.data_objects.exists(
                        "/".join([self.destination_collection, new_object_name])
                    )
                )
            finally:
                self.minio_client.remove_object(self.bucket_name, new_object_name)

    def test_s3_with_put_sync(self):
        operation = Operation.PUT_SYNC
        new_object_name = "test_s3_with_put_sync"
        event_handler_contents = test_s3_sync_operations.get_event_handler(operation)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was ingested properly.
            self.run_sync(
                self.source_path, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            try:
                self.minio_client.put_object(
                    self.bucket_name,
                    new_object_name,
                    data=io.BytesIO(new_object_name.encode()),
                    length=len(new_object_name),
                )
                self.run_sync(
                    self.source_path, self.destination_collection, event_handler_path
                )
                self.assert_ingested_contents_exist_in_irods()
                self.assertTrue(
                    self.irods_session.data_objects.exists(
                        "/".join([self.destination_collection, new_object_name])
                    )
                )
            finally:
                self.minio_client.remove_object(self.bucket_name, new_object_name)

    def test_s3_with_put_append(self):
        operation = Operation.PUT_SYNC
        new_object_name = "test_s3_with_put_append"
        event_handler_contents = test_s3_sync_operations.get_event_handler(operation)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was ingested properly.
            self.run_sync(
                self.source_path, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            try:
                self.minio_client.put_object(
                    self.bucket_name,
                    new_object_name,
                    data=io.BytesIO(new_object_name.encode()),
                    length=len(new_object_name),
                )
                self.run_sync(
                    self.source_path, self.destination_collection, event_handler_path
                )
                self.assert_ingested_contents_exist_in_irods()
                self.assertTrue(
                    self.irods_session.data_objects.exists(
                        "/".join([self.destination_collection, new_object_name])
                    )
                )
            finally:
                self.minio_client.remove_object(self.bucket_name, new_object_name)

    def test_s3_with_register_sync(self):
        operation = Operation.REGISTER_SYNC
        new_object_name = "test_s3_with_register_sync"
        event_handler_contents = test_s3_sync_operations.get_event_handler(operation)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was ingested properly.
            self.run_sync(
                self.source_path, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            try:
                self.minio_client.put_object(
                    self.bucket_name,
                    new_object_name,
                    data=io.BytesIO(new_object_name.encode()),
                    length=len(new_object_name),
                )
                self.run_sync(
                    self.source_path, self.destination_collection, event_handler_path
                )
                self.assert_ingested_contents_exist_in_irods()
                self.assertTrue(
                    self.irods_session.data_objects.exists(
                        "/".join([self.destination_collection, new_object_name])
                    )
                )
            finally:
                self.minio_client.remove_object(self.bucket_name, new_object_name)

    def test_s3_with_register_as_replica_sync(self):
        operation = Operation.REGISTER_AS_REPLICA_SYNC
        new_object_name = "test_s3_with_register_as_replica_sync"
        event_handler_contents = test_s3_sync_operations.get_event_handler(operation)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was ingested properly.
            self.run_sync(
                self.source_path, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            try:
                self.minio_client.put_object(
                    self.bucket_name,
                    new_object_name,
                    data=io.BytesIO(new_object_name.encode()),
                    length=len(new_object_name),
                )
                self.run_sync(
                    self.source_path, self.destination_collection, event_handler_path
                )
                self.assert_ingested_contents_exist_in_irods()
                self.assertTrue(
                    self.irods_session.data_objects.exists(
                        "/".join([self.destination_collection, new_object_name])
                    )
                )
            finally:
                self.minio_client.remove_object(self.bucket_name, new_object_name)


def main():
    unittest.main()


if __name__ == "__main__":
    main()
