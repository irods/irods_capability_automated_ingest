import unittest

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

from . import test_lib

# TODO(#286): Derive from the environment?
# This must be set as an environment variable in order for the Celery workers to communicate with the broker.
# Update this value if the hostname, port, or database for the Redis service needs to change.
os.environ["CELERY_BROKER_URL"] = "redis://redis:6379/0"

# These are useful to have as a global because delete modes only have an effect for sync operations.
non_sync_operations = [Operation.NO_OP, Operation.PUT]


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


def get_event_handler(operation, delete_mode):
    operation_strings = {
        Operation.NO_OP: "NO_OP",
        Operation.REGISTER_SYNC: "REGISTER_SYNC",
        Operation.REGISTER_AS_REPLICA_SYNC: "REGISTER_AS_REPLICA_SYNC",
        Operation.PUT: "PUT",
        Operation.PUT_SYNC: "PUT_SYNC",
        Operation.PUT_APPEND: "PUT_APPEND",
    }
    delete_mode_strings = {
        DeleteMode.DO_NOT_DELETE: "DO_NOT_DELETE",
        DeleteMode.UNREGISTER: "UNREGISTER",
        DeleteMode.TRASH: "TRASH",
        DeleteMode.NO_TRASH: "NO_TRASH",
    }
    return textwrap.dedent(
        f"""
        from irods_capability_automated_ingest.core import Core
        from irods_capability_automated_ingest.utils import DeleteMode, Operation
        class event_handler(Core):
            @staticmethod
            def operation(session, meta, **options):
                return Operation.{operation_strings[operation]}

            @staticmethod
            def delete_mode(meta):
                return DeleteMode.{delete_mode_strings[delete_mode]}
        """
    )


class test_delete_modes_with_sync_operations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.restart_queue_name = "delete_op_restart"
        cls.path_queue_name = "delete_op_path"
        cls.file_queue_name = "delete_op_file"
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
        cls.job_name = "test_delete_modes_job"

    @classmethod
    def tearDownClass(cls):
        test_lib.clear_redis()
        test_lib.irmtrash()
        cls.irods_session.cleanup()
        cls.workers.send_signal(signal.SIGINT)
        cls.workers.wait()

    @staticmethod
    def create_directory(directory_dict, parent=None):
        if parent:
            directory_path = os.path.join(parent, directory_dict["name"])
        else:
            directory_path = directory_dict["name"]
        os.makedirs(directory_path, exist_ok=True)
        for subdirectory in directory_dict["subdirectories"]:
            test_delete_modes_with_sync_operations.create_directory(
                subdirectory, parent=directory_path
            )
        for file in directory_dict["files"]:
            with open(os.path.join(directory_path, file), "w") as f:
                f.write(f"contents for {file}")

    def setUp(self):
        # TODO(#286): Derive /data mountpoint rather than hard-coding
        self.source_directory = tempfile.mkdtemp(dir="/data/ufs")
        self.directory_tree = {
            "name": self.source_directory,
            "subdirectories": [
                {
                    "name": "shallow_subdirectory",
                    "subdirectories": [],
                    "files": ["file_that_exists_to_activate_trash.txt"],
                },
                {
                    "name": "deep_subdirectory",
                    "subdirectories": [
                        {
                            "name": "a",
                            "subdirectories": [
                                {
                                    "name": "b",
                                    "subdirectories": [
                                        {
                                            "name": "c",
                                            "subdirectories": [],
                                            "files": [],
                                        },
                                    ],
                                    "files": [],
                                },
                            ],
                            "files": [],
                        },
                        {
                            "name": "x",
                            "subdirectories": [
                                {
                                    "name": "y",
                                    "subdirectories": [
                                        {
                                            "name": "z",
                                            "subdirectories": [],
                                            "files": [],
                                        },
                                    ],
                                    "files": [],
                                },
                            ],
                            "files": [],
                        },
                    ],
                    "files": [],
                },
            ],
            "files": ["top_level_file.txt"],
        }
        test_delete_modes_with_sync_operations.create_directory(self.directory_tree)
        # TODO(#280): The sync job fails without this sleep because the Celery workers don't yet see the directories.
        time.sleep(1)
        self.destination_collection = "/".join(
            [
                "",
                self.irods_session.zone,
                "home",
                self.irods_session.username,
                os.path.basename(self.source_directory),
            ]
        )
        self.target_subdirectory_for_removal = os.path.join(
            self.source_directory, self.directory_tree["subdirectories"][0]["name"]
        )
        self.target_subcollection_for_removal = "/".join(
            [
                self.destination_collection,
                os.path.basename(self.target_subdirectory_for_removal),
            ]
        )
        self.trash_path_for_deleted_directory = "/".join(
            [
                "",
                self.irods_session.zone,
                "trash",
                "home",
                self.irods_session.username,
                os.path.basename(self.source_directory),
                os.path.basename(self.target_subdirectory_for_removal),
            ]
        )

    def tearDown(self):
        shutil.rmtree(self.source_directory, ignore_errors=True)
        test_lib.delete_collection_if_exists(
            self.destination_collection, recurse=True, force=True
        )

    def run_sync(
        self,
        source_directory,
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
            source_directory,
            destination_collection,
            "--event_handler",
            event_handler_path,
            "--files_per_task",
            str(files_per_task),
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
        try:
            # TODO(#287): There should be more assertions about the contents of the injested collection.
            self.assertTrue(
                self.irods_session.collections.exists(
                    self.target_subcollection_for_removal
                )
            )
        except CollectionDoesNotExist:
            self.fail(
                f"Collection [{self.destination_collection}] not created by sync job."
            )

    def assert_deleted_directory_resulted_in_deleted_collection(
        self, collection_exists=False, collection_in_trash=False
    ):
        try:
            self.assertEqual(
                collection_exists,
                self.irods_session.collections.exists(
                    self.target_subcollection_for_removal
                ),
            )
            self.assertEqual(
                collection_in_trash,
                self.irods_session.collections.exists(
                    self.trash_path_for_deleted_directory
                ),
            )
        except CollectionDoesNotExist:
            self.fail(
                f"Collection [{self.destination_collection}] not created by sync job."
            )

    def do_DO_NOT_DELETE_does_not_delete_collections(self, operation):
        # This test ensures that the provided sync operation is compatible with the DO_NOT_DELETE delete mode.
        delete_mode = DeleteMode.DO_NOT_DELETE
        # Error for non-sync operations because delete modes only take effect on a sync.
        if operation in non_sync_operations:
            raise ValueError(
                f"Provided operation [{operation}] does not sync, so it cannot be used here."
            )
        event_handler_contents = get_event_handler(operation, delete_mode)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was registered properly.
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            # Now delete a subdirectory from the source...
            shutil.rmtree(self.target_subdirectory_for_removal)
            # Run the job again (sync) and confirm that the deleted directory sync did not cause the collection to be
            # deleted.
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            self.assert_deleted_directory_resulted_in_deleted_collection(
                collection_exists=True, collection_in_trash=False
            )

    def do_UNREGISTER_deletes_collections(self, operation):
        # This test ensures that every sync operation is compatible with the UNREGISTER delete mode.
        delete_mode = DeleteMode.UNREGISTER
        # Skip non-sync operations because delete modes only take effect on a sync.
        if operation in non_sync_operations:
            raise ValueError(
                f"Provided operation [{operation}] does not sync, so it cannot be used here."
            )
        event_handler_contents = get_event_handler(operation, delete_mode)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was registered properly.
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            # TODO(#287): Run a query to get the physical path of the data object in the target subdirectory for removal.
            # Now delete a subdirectory from the source...
            shutil.rmtree(self.target_subdirectory_for_removal)
            # Run the job again (sync) and confirm that the deleted directory sync caused the collection to be deleted
            # and that it did not end up in the trash.
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            self.assert_deleted_directory_resulted_in_deleted_collection(
                collection_exists=False, collection_in_trash=False
            )
            # TODO(#287): Also confirm that the data remains in storage. The physical path is fetched beforehand.

    def do_TRASH_or_NO_TRASH_deletes_collections(self, operation, delete_mode):
        event_handler_contents = get_event_handler(operation, delete_mode)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was registered properly.
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            # Now delete a subdirectory from the source...
            shutil.rmtree(self.target_subdirectory_for_removal)
            # Run the job again (sync) and confirm that the deleted directory sync caused the collection to be deleted
            # and that it ends up in the trash (or not).
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            expect_collection_to_exist = False
            expect_collection_in_trash = DeleteMode.TRASH == delete_mode
            self.assert_deleted_directory_resulted_in_deleted_collection(
                collection_exists=expect_collection_to_exist,
                collection_in_trash=expect_collection_in_trash,
            )

    def do_incompatible_operation_and_delete_mode(self, operation, delete_mode):
        # This test ensures that the provided sync operation is incompatible with the provided delete mode.
        event_handler_contents = get_event_handler(operation, delete_mode)
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that the main task fails.
            self.run_sync(
                self.source_directory,
                self.destination_collection,
                event_handler_path,
                expected_failure_count=1,
            )
            # The failure should have occurred before any work happened.
            self.assertFalse(
                self.irods_session.collections.exists(self.destination_collection)
            )

    # The following tests are not implemented as sub-tests because this test class relies heavily on the setUp and
    # tearDown methods to ensure that the source and destination for the sync are in their expected starting conditions.

    @unittest.skip(
        "NO_OP cannot sync, so I'm not sure how we test this. Regular NO_OP ingest is tested elsewhere."
    )
    def test_NO_OP_and_DO_NOT_DELETE_are_incompatible(self):
        # Case 0
        pass

    def test_NO_OP_and_UNREGISTER_are_incompatible(self):
        # Case 1
        self.do_incompatible_operation_and_delete_mode(
            Operation.NO_OP, DeleteMode.UNREGISTER
        )

    def test_NO_OP_and_TRASH_are_incompatible(self):
        # Case 2
        self.do_incompatible_operation_and_delete_mode(
            Operation.NO_OP, DeleteMode.TRASH
        )

    def test_NO_OP_and_NO_TRASH_are_incompatible(self):
        # Case 3
        self.do_incompatible_operation_and_delete_mode(
            Operation.NO_OP, DeleteMode.NO_TRASH
        )

    def test_REGISTER_SYNC_and_DO_NOT_DELETE_does_not_delete_collections(self):
        # Case 4
        self.do_DO_NOT_DELETE_does_not_delete_collections(Operation.REGISTER_SYNC)

    def test_REGISTER_SYNC_and_UNREGISTER_deletes_collections(self):
        # Case 5
        self.do_UNREGISTER_deletes_collections(Operation.REGISTER_SYNC)

    def test_REGISTER_SYNC_and_TRASH_are_incompatible(self):
        # Case 6
        self.do_incompatible_operation_and_delete_mode(
            Operation.REGISTER_SYNC, DeleteMode.TRASH
        )

    def test_REGISTER_SYNC_and_NO_TRASH_are_incompatible(self):
        # Case 7
        self.do_incompatible_operation_and_delete_mode(
            Operation.REGISTER_SYNC, DeleteMode.NO_TRASH
        )

    def test_REGISTER_AS_REPLICA_SYNC_and_DO_NOT_DELETE_does_not_delete_collections(
        self,
    ):
        # Case 8
        self.do_DO_NOT_DELETE_does_not_delete_collections(
            Operation.REGISTER_AS_REPLICA_SYNC
        )

    def test_REGISTER_AS_REPLICA_SYNC_and_UNREGISTER_deletes_collections(self):
        # Case 9
        self.do_UNREGISTER_deletes_collections(Operation.REGISTER_AS_REPLICA_SYNC)

    def test_REGISTER_AS_REPLICA_SYNC_and_TRASH_are_incompatible(self):
        # Case 10
        self.do_incompatible_operation_and_delete_mode(
            Operation.REGISTER_AS_REPLICA_SYNC, DeleteMode.TRASH
        )

    def test_REGISTER_AS_REPLICA_SYNC_and_NO_TRASH_are_incompatible(self):
        # Case 11
        self.do_incompatible_operation_and_delete_mode(
            Operation.REGISTER_AS_REPLICA_SYNC, DeleteMode.NO_TRASH
        )

    @unittest.skip(
        "PUT cannot sync, so I'm not sure how we test this. Regular PUT ingest is tested elsewhere."
    )
    def test_PUT_and_DO_NOT_DELETE_are_incompatible(self):
        # Case 12
        pass

    def test_PUT_and_UNREGISTER_are_incompatible(self):
        # Case 13
        self.do_incompatible_operation_and_delete_mode(
            Operation.PUT, DeleteMode.UNREGISTER
        )

    def test_PUT_and_TRASH_are_incompatible(self):
        # Case 14
        self.do_incompatible_operation_and_delete_mode(Operation.PUT, DeleteMode.TRASH)

    def test_PUT_and_NO_TRASH_are_incompatible(self):
        # Case 15
        self.do_incompatible_operation_and_delete_mode(
            Operation.PUT, DeleteMode.NO_TRASH
        )

    def test_PUT_SYNC_and_DO_NOT_DELETE_does_not_delete_collections(self):
        # Case 16
        self.do_DO_NOT_DELETE_does_not_delete_collections(Operation.PUT_SYNC)

    def test_PUT_SYNC_and_UNREGISTER_are_incompatible(self):
        # Case 17
        self.do_incompatible_operation_and_delete_mode(
            Operation.PUT_SYNC, DeleteMode.UNREGISTER
        )

    def test_PUT_SYNC_and_TRASH_deletes_collections(self):
        # Case 18
        self.do_TRASH_or_NO_TRASH_deletes_collections(
            Operation.PUT_SYNC, DeleteMode.TRASH
        )

    def test_PUT_SYNC_and_NO_TRASH_deletes_collections(self):
        # Case 19
        self.do_TRASH_or_NO_TRASH_deletes_collections(
            Operation.PUT_SYNC, DeleteMode.NO_TRASH
        )

    def test_PUT_APPEND_and_DO_NOT_DELETE_does_not_delete_collections(self):
        # Case 20
        self.do_DO_NOT_DELETE_does_not_delete_collections(Operation.PUT_APPEND)

    def test_PUT_SYNC_and_UNREGISTER_are_incompatible(self):
        # Case 21
        self.do_incompatible_operation_and_delete_mode(
            Operation.PUT_SYNC, DeleteMode.UNREGISTER
        )

    def test_PUT_APPEND_and_TRASH_deletes_collections(self):
        # Case 22
        self.do_TRASH_or_NO_TRASH_deletes_collections(
            Operation.PUT_APPEND, DeleteMode.TRASH
        )

    def test_PUT_APPEND_and_NO_TRASH_deletes_collections(self):
        # Case 23
        self.do_TRASH_or_NO_TRASH_deletes_collections(
            Operation.PUT_APPEND, DeleteMode.NO_TRASH
        )

    def test_deleting_all_files_from_directory_but_not_the_directory_itself_does_not_delete_collection__issue_288(
        self,
    ):
        event_handler_contents = get_event_handler(
            Operation.REGISTER_SYNC, DeleteMode.UNREGISTER
        )
        target_files_for_removal = [
            os.path.join(self.target_subdirectory_for_removal, file)
            for file in self.directory_tree["subdirectories"][0]["files"]
        ]
        with tempfile.NamedTemporaryFile() as tf:
            event_handler_path = tf.name
            with open(event_handler_path, "w") as f:
                f.write(event_handler_contents)
            # Run the first sync and confirm that everything was registered properly.
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            self.assert_ingested_contents_exist_in_irods()
            for target_file in target_files_for_removal:
                file = os.path.basename(target_file)
                self.assertTrue(
                    self.irods_session.data_objects.exists(
                        "/".join([self.target_subcollection_for_removal, file])
                    ),
                )
            # TODO(#287): Run a query to get the physical path of the data object in the target subdirectory for removal.
            # Now delete all the files in a subdirectory from the source, but not the subdirectory itself...
            for file in target_files_for_removal:
                os.unlink(file)
            self.assertTrue(os.path.exists(self.target_subdirectory_for_removal))
            # Run the job again (sync) and confirm that the deleted files sync caused the collection NOT to be deleted.
            self.run_sync(
                self.source_directory, self.destination_collection, event_handler_path
            )
            # The data objects in the collection should no longer exist.
            for target_file in target_files_for_removal:
                file = os.path.basename(target_file)
                self.assertFalse(
                    self.irods_session.data_objects.exists(
                        "/".join([self.target_subcollection_for_removal, file])
                    ),
                )
            # The collection should still exist.
            self.assertTrue(
                self.irods_session.collections.exists(
                    self.target_subcollection_for_removal
                ),
            )


def main():
    unittest.main()


if __name__ == "__main__":
    main()
