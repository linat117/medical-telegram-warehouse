from sqlalchemy.orm import Session
from sqlalchemy import text
from api import schemas

# Endpoint 1 - top products
def get_top_products(db: Session, limit: int = 10):
    """
    Get top products by message views (treating messages as products).
    For a more sophisticated implementation, this would extract actual product names
    from message text using NLP or pattern matching.
    """
    try:
        # Query from the fact table to get top messages by views
        # In a real implementation, you'd extract product names from message_text
        query = text("""
            SELECT 
                SUBSTRING(message_text FROM 1 FOR 50) AS product,
                view_count AS mentions
            FROM raw_raw.fct_messages
            WHERE message_text IS NOT NULL 
                AND message_text != ''
                AND view_count > 0
            ORDER BY view_count DESC
            LIMIT :limit
        """)
        result = db.execute(query, {"limit": limit}).fetchall()
        
        # Convert Row objects to dictionaries, then to Pydantic models
        return [
            schemas.TopProduct(
                product=row.product if row.product else "N/A",
                mentions=int(row.mentions) if row.mentions else 0
            )
            for row in result
        ]
    except Exception as e:
        # Log the error and raise HTTPException
        print(f"Error in get_top_products: {str(e)}")
        raise

# Endpoint 2 - channel activity
def get_channel_activity(db: Session, channel_name: str):
    """
    Get channel activity over time.
    """
    try:
        query = text("""
            SELECT 
                d.full_date::text AS date,
                COUNT(m.message_id) AS message_count
            FROM raw_raw.fct_messages m
            JOIN raw_raw.dim_channels c ON m.channel_key = c.channel_key
            JOIN raw_raw.dim_dates d ON m.date_key = d.full_date
            WHERE c.channel_name = :channel
            GROUP BY d.full_date
            ORDER BY d.full_date
        """)
        result = db.execute(query, {"channel": channel_name}).fetchall()
        
        return [
            schemas.ChannelActivity(
                date=str(row.date),
                message_count=int(row.message_count)
            )
            for row in result
        ]
    except Exception as e:
        print(f"Error in get_channel_activity: {str(e)}")
        raise

# Endpoint 3 - message search
def search_messages(db: Session, keyword: str, limit: int = 10):
    """
    Search messages by keyword in message text.
    """
    try:
        query = text("""
            SELECT 
                m.message_id,
                c.channel_name,
                m.message_text,
                d.full_date::text AS date
            FROM raw_raw.fct_messages m
            JOIN raw_raw.dim_channels c ON m.channel_key = c.channel_key
            JOIN raw_raw.dim_dates d ON m.date_key = d.full_date
            WHERE m.message_text ILIKE :keyword
            ORDER BY m.view_count DESC
            LIMIT :limit
        """)
        result = db.execute(
            query,
            {"keyword": f"%{keyword}%", "limit": limit}
        ).fetchall()
        
        return [
            schemas.MessageSearchResult(
                message_id=int(row.message_id),
                channel_name=str(row.channel_name),
                message_text=str(row.message_text) if row.message_text else "",
                date=str(row.date)
            )
            for row in result
        ]
    except Exception as e:
        print(f"Error in search_messages: {str(e)}")
        raise

# Endpoint 4 - visual content stats
def get_visual_content_stats(db: Session):
    """
    Get visual content statistics per channel.
    """
    try:
        query = text("""
            SELECT 
                c.channel_name,
                SUM(CASE WHEN m.has_image THEN 1 ELSE 0 END) AS image_count,
                COUNT(*) AS total_messages
            FROM raw_raw.fct_messages m
            JOIN raw_raw.dim_channels c ON m.channel_key = c.channel_key
            GROUP BY c.channel_name
            ORDER BY image_count DESC
        """)
        result = db.execute(query).fetchall()
        
        return [
            schemas.VisualContentStats(
                channel_name=str(row.channel_name),
                image_count=int(row.image_count) if row.image_count else 0,
                total_messages=int(row.total_messages) if row.total_messages else 0
            )
            for row in result
        ]
    except Exception as e:
        print(f"Error in get_visual_content_stats: {str(e)}")
        raise
