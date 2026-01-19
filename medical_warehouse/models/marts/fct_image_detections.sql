WITH detections AS (
    SELECT
        image_name,
        image_category,
        confidence_score
    FROM {{ ref('stg_yolo_detections') }}
),

messages AS (
    SELECT
        message_id,
        image_name,
        channel_key,
        date_key
    FROM {{ ref('fct_messages') }}
)

SELECT
    m.message_id,
    m.channel_key,
    m.date_key,
    d.image_category,
    d.confidence_score
FROM messages m
JOIN detections d
    ON m.image_name = d.image_name
