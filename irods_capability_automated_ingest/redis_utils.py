from redis import StrictRedis, ConnectionPool

redis_connection_pool_map = {}


def get_redis(config):
    redis_config = config["redis"]
    host = redis_config["host"]
    port = redis_config["port"]
    db = redis_config["db"]
    url = "redis://" + host + ":" + str(port) + "/" + str(db)
    pool = redis_connection_pool_map.get(url)
    if pool is None:
        pool = ConnectionPool(host=host, port=port, db=db)
        redis_connection_pool_map[url] = pool

    return StrictRedis(connection_pool=pool)
