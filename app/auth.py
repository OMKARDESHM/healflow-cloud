from passlib.context import CryptContext
from sqlalchemy.orm import Session

from . import models

# Use pbkdf2_sha256 – no bcrypt issues, safe for MVP
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    if password is None:
        password = ""
    return pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    if password is None:
        password = ""
    return pwd_context.verify(password, hashed)


def get_user_by_email(db: Session, email: str):
    return db.query(models.User).filter(models.User.email == email).first()


def create_user(db: Session, email: str, password: str):
    user = models.User(
        email=email,
        password_hash=hash_password(password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def authenticate_user(db: Session, email: str, password: str):
    user = get_user_by_email(db, email)
    if not user:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user
