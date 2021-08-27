import importlib
import sys
from .redis_key import redis_key_handle
from .sync_utils import get_redis

class custom_event_handler(object):
    def __init__(self, meta):
        self.meta = meta.copy()
        self.logger = self.meta['config']['log']

    def get_module(self, rtn_mod_and_class = False):   # get_ev_handler_class or something
        r = get_redis(self.meta['config'])
        key = 'event_handler'
        #h = self.meta.get(key)
        #if h is None:
        #    return (None, None) if rtn_mod_and_class else None

        event_handler_key = redis_key_handle(r, "custom_event_handler", self.meta['job_name'])
        content_string = event_handler_key.get_value()
        with open("/tmp/event_handler.py", "w") as eh:
            eh.write(content_string.decode("utf-8"))

        sys.path.insert(0, "/tmp/")
        #import event_handler

        mod = importlib.import_module("event_handler")
        if mod is None:
            return (None, None) if rtn_mod_and_class else None

        cls = getattr(mod, key, None)
        if rtn_mod_and_class:
            return (mod,cls)

        return cls

    def hasattr(self, attr):
        module = self.get_module()
        return module is not None and hasattr(module, attr)

    def call(self, hdlr, logger, func, *args, **options):
        (mod,cls) = self.get_module(rtn_mod_and_class=True)
        args = (mod,) + tuple(args)

        if self.hasattr(hdlr):
            logger.debug("calling [" + hdlr + "] in event handler: args = " + str(args) + ", options = " + str(options))
            getattr(cls, hdlr)(func, *args, **options)
        else:
            func(*args, **options)

    # attribute getters
    def max_retries(self):
        if self.hasattr('max_retries'):
            module = self.get_module()
            return module.max_retries(module, self.logger, self.meta)
        return 0

    def timeout(self):
        if self.hasattr('timeout'):
            module = self.get_module()
            return module.timeout(module, self.logger, self.meta)
        return 3600

    def delay(self, retries):
        if self.hasattr('delay'):
            module = self.get_module()
            return module.delay(module, self.logger, self.meta, retries)
        return 0

    def operation(self, session, **options):
        if self.hasattr("operation"):
            return self.get_module().operation(session, self.meta, **options)

        from .utils import Operation
        return Operation.REGISTER_SYNC
        #return None

    def to_resource(self, session, **options):
        if self.hasattr("to_resource"):
            return self.get_module().to_resource(session, self.meta, **options)
        return None

    def target_path(self, session, **options):
        if self.hasattr("target_path"):
            return self.get_module().target_path(session, self.meta, **options)
        return None

