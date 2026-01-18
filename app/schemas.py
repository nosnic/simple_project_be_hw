from pydantic import BaseModel


class ItemCreate(BaseModel):
    name: str


class ItemRead(BaseModel):
    id: int
    name: str
