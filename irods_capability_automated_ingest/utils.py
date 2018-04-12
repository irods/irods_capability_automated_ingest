from enum import Enum

class Operation(Enum):
    REGISTER = 0
    REGISTER_AS_REPLICA = 1
    PUT = 2
    SYNC = 3
    APPEND = 4

