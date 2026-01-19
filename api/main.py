from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import traceback

from api.database import get_db
from api import crud, schemas

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="Analytical API exposing data warehouse insights",
    version="1.0.0"
)


# endpoint 1 - top products

@app.get(
    "/api/reports/top-products",
    response_model=List[schemas.TopProduct],
    description="Returns the most frequently mentioned products"
)
def top_products(limit: int = 10, db: Session = Depends(get_db)):
    try:
        return crud.get_top_products(db, limit)
    except Exception as e:
        error_msg = str(e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {error_msg}")

# endpoint 2 - channel activity

@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=List[schemas.ChannelActivity],
    description="Returns posting activity over time for a channel"
)
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    try:
        data = crud.get_channel_activity(db, channel_name)
        if not data:
            raise HTTPException(status_code=404, detail="Channel not found")
        return data
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {error_msg}")
# endpoint 3 - message search

@app.get(
    "/api/search/messages",
    response_model=List[schemas.MessageSearchResult],
    description="Search messages by keyword"
)
def search_messages(query: str, limit: int = 20, db: Session = Depends(get_db)):
    try:
        return crud.search_messages(db, query, limit)
    except Exception as e:
        error_msg = str(e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {error_msg}")


# endpoint 4 - visual content stats

@app.get(
    "/api/reports/visual-content",
    response_model=List[schemas.VisualContentStats],
    description="Returns image usage statistics per channel"
)
def visual_content_stats(db: Session = Depends(get_db)):
    try:
        return crud.get_visual_content_stats(db)
    except Exception as e:
        error_msg = str(e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {error_msg}")
