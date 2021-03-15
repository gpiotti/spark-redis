import json

from fastapi import FastAPI, HTTPException

from app.services import r
from app.settings import AppConfig

config = AppConfig()
app = FastAPI()


@app.get("/user_features/{user_id}")
async def user_features(user_id: int):
    resp = r.get(user_id)
    if resp:
        return json.loads(r.get(user_id))
    else:
        raise HTTPException(status_code=404, detail="User not found")


@app.get("/ping/")
def ping():
    return "pong"
