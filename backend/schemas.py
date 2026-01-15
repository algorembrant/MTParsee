from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class GoogleAuth(BaseModel):
    token: str


class UserBase(BaseModel):
    email: str
    name: str


class UserCreate(UserBase):
    google_id: str


class User(UserBase):
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


class UploadBase(BaseModel):
    filename: str
    status: str


class Upload(UploadBase):
    id: int
    upload_date: datetime
    user_id: Optional[int]
    
    class Config:
        from_attributes = True