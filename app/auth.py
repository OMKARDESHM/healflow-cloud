from typing import Optional
from sqlalchemy.orm import Session
from passlib.hash import sha256_crypt

from . import models


def hash_password(password: str) -> str:
    # sha256_crypt has no bcrypt native issues and is good enough for this project
    return sha256_crypt.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return sha256_crypt.verify(password, password_hash)


def get_user_by_username(db: Session, username: str) -> Optional[models.User]:
    return db.query(models.User).filter(models.User.username == username).first()


def create_user(db: Session, username: str, password: str) -> models.User:
    user = models.User(
        username=username,
        password_hash=hash_password(password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user
