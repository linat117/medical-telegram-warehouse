SELECT
    m.message_id,
    c.channel_key,
    d.full_date AS date_key,
    m.message_text,
    m.message_length,
    m.views AS view_count,
    m.forwards AS forward_count,
    m.has_image,
    CASE
        WHEN m.has_image THEN m.message_id || '.jpg'
        ELSE NULL
    END AS image_name
FROM {{ ref('stg_telegram_messages') }} AS m
LEFT JOIN {{ ref('dim_channels') }} AS c
    ON m.channel_name = c.channel_name
LEFT JOIN {{ ref('dim_dates') }} AS d
    ON m.message_date::date = d.full_date
