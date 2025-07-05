import redis
from config.settings import REDIS_HOST, REDIS_PORT

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
