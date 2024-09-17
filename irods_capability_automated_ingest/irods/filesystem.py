from . import irods_utils
from .. import custom_event_handler
from ..utils import Operation

from irods.models import Resource, DataObject, Collection
import irods.keywords as kw

import base64
import os


def append_to_data_object(
    session, logger, source_physical_path, destination_logical_path, **options
):
    BUFFER_SIZE = 1024
    logger.info(
        f"appending object {source_physical_path} from local filesystem, options = {options}"
    )
    tsize = irods_utils.size(session, destination_logical_path)
    destination_fd = session.data_objects.open(destination_logical_path, "a", **options)
    destination_fd.seek(tsize)
    with open(source_physical_path, "rb") as source_fd:
        source_fd.seek(tsize)
        while True:
            buf = source_fd.read(BUFFER_SIZE)
            if buf == b"":
                break
            destination_fd.write(buf)
    destination_fd.close()
    logger.info("succeeded", task="irods_FSappend_file", path=source_physical_path)


def register_file(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")

    event_handler = custom_event_handler.custom_event_handler(meta)
    if event_handler is None:
        phypath_to_register_in_catalog = None
    else:
        phypath_to_register_in_catalog = event_handler.target_path(session, **options)
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None and "unicode_error_filename" in meta:
            phypath_to_register_in_catalog = os.path.join(
                source_physical_fullpath, meta["unicode_error_filename"]
            )
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    options[kw.DATA_SIZE_KW] = str(meta["size"])
    options[kw.DATA_MODIFY_KW] = str(int(meta["mtime"]))

    logger.info(
        "registering object {}, source_physical_fullpath: {}, options = {}".format(
            dest_dataobj_logical_fullpath, source_physical_fullpath, options
        )
    )
    session.data_objects.register(
        phypath_to_register_in_catalog, dest_dataobj_logical_fullpath, **options
    )

    logger.info("succeeded", task="irods_register_file", path=source_physical_fullpath)

    irods_utils.annotate_metadata_for_special_data_objs(
        meta, session, source_physical_fullpath, dest_dataobj_logical_fullpath
    )


def upload_file(hdlr_mod, logger, session, meta, op, **options):
    """
    Function called from sync_irods.sync_file and sync_irods.upload_file for local files

    If called from sync_irods.sync_file, determines if it should be an append operation, or a put.
    If called from sync_irods.upload_file, simply puts the file into iRODS

    op: the type of operation (one of Operation.PUT, Operation.PUT_APPEND, Operation.PUT_SYNC)
    """
    # TODO: Check for op here

    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")
    event_handler = custom_event_handler.custom_event_handler(meta)
    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")
    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    logger.info(
        f"uploading object {source_physical_fullpath} from local filesystem, options = {options}"
    )
    session.data_objects.put(
        source_physical_fullpath, dest_dataobj_logical_fullpath, **options
    )
    logger.info("succeeded", task="irods_FSupload_file", path=source_physical_fullpath)

    irods_utils.annotate_metadata_for_special_data_objs(
        meta, session, source_physical_fullpath, dest_dataobj_logical_fullpath
    )


def no_op(hdlr_mod, logger, session, meta, **options):
    pass


def sync_file(hdlr_mod, logger, session, meta, op, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    b64_path_str = meta.get("b64_path_str")

    event_handler = custom_event_handler.custom_event_handler(meta)
    resc_name = event_handler.to_resource(session, **options)
    if resc_name is not None:
        options["destRescName"] = resc_name

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    logger.info(
        "syncing object %s, options = %s" % (dest_dataobj_logical_fullpath, options)
    )

    # TODO: Issue #208 - This is incorrect -- where is the register function ??
    # Investigate behavior of sync_file when op is None
    if op is None:
        op = Operation.REGISTER_SYNC

    # PUT_APPEND with existing file. sync_file assumes the file already exists.
    if op == Operation.PUT_APPEND:
        append_to_data_object(
            session,
            logger,
            source_physical_fullpath,
            dest_dataobj_logical_fullpath,
            **options,
        )

    # If data object doesn't exist, just do a standard put
    # If data object does exist, we know op=PUT_SYNC, and we re-copy whole file again, so it is fine also
    else:
        logger.info(
            f"uploading object {source_physical_fullpath} from local filesystem, options = {options}"
        )
        session.data_objects.put(
            source_physical_fullpath, dest_dataobj_logical_fullpath, **options
        )
        logger.info(
            "succeeded", task="irods_FSupload_file", path=source_physical_fullpath
        )


def update_metadata(hdlr_mod, logger, session, meta, **options):
    dest_dataobj_logical_fullpath = meta["target"]
    source_physical_fullpath = meta["path"]
    event_handler = custom_event_handler.custom_event_handler(meta)
    phypath_to_register_in_catalog = event_handler.target_path(session, **options)
    b64_path_str = meta.get("b64_path_str")
    if phypath_to_register_in_catalog is None:
        if b64_path_str is not None and "unicode_error_filename" in meta:
            # Append generated filename to truncated fullpath because it failed to encode
            # TODO(#250): This will not work on Windows.
            phypath_to_register_in_catalog = os.path.join(
                source_physical_fullpath, meta["unicode_error_filename"]
            )
        else:
            phypath_to_register_in_catalog = source_physical_fullpath

    if b64_path_str is not None:
        source_physical_fullpath = base64.b64decode(b64_path_str)

    size = int(meta["size"])
    mtime = int(meta["mtime"])
    logger.info(
        f"updating object: {dest_dataobj_logical_fullpath}, options = {options}"
    )

    data_obj_info = {"objPath": dest_dataobj_logical_fullpath}

    outdated_repl_nums = []
    found = False

    resc_name = event_handler.to_resource(session, **options)
    if resc_name is None:
        found = True
    else:
        for row in session.query(
            Resource.name, DataObject.path, DataObject.replica_number
        ).filter(
            # TODO(#250): This will not work on Windows.
            DataObject.name == os.path.basename(dest_dataobj_logical_fullpath),
            Collection.name == os.path.dirname(dest_dataobj_logical_fullpath),
        ):
            if row[DataObject.path] == phypath_to_register_in_catalog:
                if irods_utils.child_of(session, row[Resource.name], resc_name):
                    found = True
                    repl_num = row[DataObject.replica_number]
                    data_obj_info["replNum"] = repl_num
                    continue

    if not found:
        if b64_path_str is not None:
            logger.error(
                "updating object: wrong resource or path, "
                "dest_dataobj_logical_fullpath = {}, phypath_to_register_in_catalog = {}, options = {}".format(
                    dest_dataobj_logical_fullpath,
                    phypath_to_register_in_catalog,
                    str(options),
                )
            )
        else:
            logger.error(
                "updating object: wrong resource or path, "
                "dest_dataobj_logical_fullpath = {}, source_physical_fullpath = {}, "
                "phypath_to_register_in_catalog = {}, options = {}".format(
                    dest_dataobj_logical_fullpath,
                    source_physical_fullpath,
                    phypath_to_register_in_catalog,
                    str(options),
                )
            )
        raise Exception("wrong resource or path")

    session.data_objects.modDataObjMeta(
        data_obj_info,
        {"dataSize": size, "dataModify": mtime, "allReplStatus": 1},
        **options,
    )

    if b64_path_str is not None:
        logger.info(
            "succeeded",
            task="irods_update_metadata",
            path=phypath_to_register_in_catalog,
        )
    else:
        logger.info(
            "succeeded", task="irods_update_metadata", path=source_physical_fullpath
        )


def sync_file_meta(hdlr_mod, logger, session, meta, **options):
    pass


def sync_data_from_file(hdlr_mod, meta, logger, content, **options):
    target = meta["target"]
    path = meta["path"]

    event_handler = custom_event_handler.custom_event_handler(meta)
    session = irods_utils.irods_session(
        event_handler.get_module(), meta, logger, **options
    )

    if meta.get("initial_ingest"):
        # If the initial_ingest option has been specified, no checking is done for the existence
        # of the logical path for performance reasons. If the option is specified and the logical
        # path exists, an error will occur; this behavior is expected.
        exists = False
    else:
        exists = session.data_objects.exists(target)
        if not exists and session.collections.exists(target):
            raise Exception(f"sync: cannot sync file {path} to collection {target}")

    op = event_handler.operation(session, **options)

    if op == Operation.NO_OP:
        if not exists:
            event_handler.call(
                "on_data_obj_create", logger, no_op, logger, session, meta, **options
            )
        else:
            event_handler.call(
                "on_data_obj_modify", logger, no_op, logger, session, meta, **options
            )
    else:
        createRepl = False
        if op is None:
            op = Operation.REGISTER_SYNC
        elif exists and op == Operation.REGISTER_AS_REPLICA_SYNC:
            resc_name = event_handler.to_resource(session, **options)
            if resc_name is None:
                raise Exception("no resource name defined")

            found = False
            foundPath = False
            for replica in session.data_objects.get(target).replicas:
                if irods_utils.child_of(session, replica.resource_name, resc_name):
                    found = True
                    if replica.path == path:
                        foundPath = True
            if not found:
                createRepl = True
            elif not foundPath:
                raise Exception(
                    f"Data object [{target}] has at least one replica under resource [{resc_name}], but none of the replicas have physical paths which match [{path}]."
                )

        put = op in [Operation.PUT, Operation.PUT_SYNC, Operation.PUT_APPEND]

        if not exists:
            meta2 = meta.copy()
            # TODO(#250): This will not work on Windows.
            meta2["target"] = os.path.dirname(target)
            if "b64_path_str" not in meta2:
                # TODO: This will not work on Windows.
                meta2["path"] = os.path.dirname(path)
            irods_utils.create_dirs(logger, session, meta2, **options)
            if put:
                event_handler.call(
                    "on_data_obj_create",
                    logger,
                    upload_file,
                    logger,
                    session,
                    meta,
                    op,
                    **options,
                )
            else:
                event_handler.call(
                    "on_data_obj_create",
                    logger,
                    register_file,
                    logger,
                    session,
                    meta,
                    **options,
                )
        elif createRepl:
            options["regRepl"] = ""

            event_handler.call(
                "on_data_obj_create",
                logger,
                register_file,
                logger,
                session,
                meta,
                **options,
            )
        elif content:
            if put:
                sync = op in [Operation.PUT_SYNC, Operation.PUT_APPEND]
                if sync:
                    event_handler.call(
                        "on_data_obj_modify",
                        logger,
                        sync_file,
                        logger,
                        session,
                        meta,
                        op,
                        **options,
                    )
            else:
                event_handler.call(
                    "on_data_obj_modify",
                    logger,
                    update_metadata,
                    logger,
                    session,
                    meta,
                    **options,
                )
        else:
            event_handler.call(
                "on_data_obj_modify",
                logger,
                sync_file_meta,
                logger,
                session,
                meta,
                **options,
            )

    irods_utils.start_timer()


def sync_metadata_from_file(hdlr_mod, meta, logger, **options):
    sync_data_from_file(hdlr_mod, meta, logger, False, **options)


def sync_dir_meta(hdlr_mod, logger, session, meta, **options):
    pass


def sync_data_from_dir(hdlr_mod, meta, logger, content, **options):
    target = meta["target"]
    path = meta["path"]

    event_handler = custom_event_handler.custom_event_handler(meta)
    session = irods_utils.irods_session(
        event_handler.get_module(), meta, logger, **options
    )
    exists = session.collections.exists(target)

    # TODO(#208): Should we default to REGISTER_SYNC?
    op = event_handler.operation(session, **options) or Operation.REGISTER_SYNC
    if op == Operation.NO_OP:
        operation_name = "on_coll_modify" if exists else "on_coll_create"
        event_handler.call(
            operation_name, logger, no_op, logger, session, meta, **options
        )
    else:
        if not exists:
            irods_utils.create_dirs(logger, session, meta, **options)
        else:
            event_handler.call(
                "on_coll_modify",
                logger,
                sync_dir_meta,
                logger,
                session,
                meta,
                **options,
            )
    irods_utils.start_timer()


def sync_metadata_from_dir(hdlr_mod, meta, logger, **options):
    sync_data_from_dir(hdlr_mod, meta, logger, False, **options)
