from pydantic import BaseModel
from typing import List


class TopProduct(BaseModel):
    product: str
    mentions: int

    class Config:
        orm_mode = True


class ChannelActivity(BaseModel):
    date: str
    message_count: int

class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    date: str

class VisualContentStats(BaseModel):
    channel_name: str
    image_count: int
    total_messages: int
