from enum import Enum

class Operation(Enum):
    REGISTER_SYNC = 0
    REGISTER_AS_REPLICA_SYNC = 1
    PUT = 2
    PUT_SYNC = 3
    PUT_APPEND = 4

