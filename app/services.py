import redis

from app.settings import AppConfig

cfg = AppConfig()

r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT, db=0)
