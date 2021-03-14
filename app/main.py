from fastapi import FastAPI
from app.settings import AppConfig
import redis

config = AppConfig()
app = FastAPI()

r = redis.Redis(host='redis', port=6379, db=0)

@app.get("/user_features/")
async def user_features(user: int):
    return r.get(user)

@app.get("/ping/")
def ping():
    return 'pong'



