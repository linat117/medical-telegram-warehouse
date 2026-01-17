WITH source AS (
    SELECT *
    FROM raw.telegram_messages
)

SELECT
    message_id,
    LOWER(channel_name) AS channel_name,
    message_date::timestamp AS message_date,
    message_text,
    has_media::boolean AS has_media,
    image_path,
    COALESCE(views, 0) AS views,
    COALESCE(forwards, 0) AS forwards,
    LENGTH(message_text) AS message_length,
    CASE 
        WHEN has_media THEN TRUE 
        ELSE FALSE 
    END AS has_image
FROM source
WHERE message_text IS NOT NULL
