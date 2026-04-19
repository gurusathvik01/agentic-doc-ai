from fastapi import APIRouter
from pydantic import BaseModel
from db import users_collection
from passlib.hash import bcrypt
import jwt
import os

router = APIRouter()

SECRET = "secret123"


class User(BaseModel):
    username: str
    password: str


@router.post("/register")
def register(user: User):
    hashed = bcrypt.hash(user.password)

    users_collection.insert_one({
        "username": user.username,
        "password": hashed
    })

    return {"message": "User created"}


@router.post("/login")
def login(user: User):
    db_user = users_collection.find_one({"username": user.username})

    if not db_user or not bcrypt.verify(user.password, db_user["password"]):
        return {"error": "Invalid credentials"}

    token = jwt.encode({"user": user.username}, SECRET, algorithm="HS256")

    return {"token": token}