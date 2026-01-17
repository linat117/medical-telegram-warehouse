SELECT
    m.message_id,
    c.channel_key,
    d.full_date AS date_key,
    m.message_text,
    m.message_length,
    m.views AS view_count,
    m.forwards AS forward_count,
    m.has_image
FROM {{ ref('stg_telegram_messages') }} m
LEFT JOIN {{ ref('dim_channels') }} c
    ON m.channel_name = c.channel_name
LEFT JOIN {{ ref('dim_dates') }} d
    ON m.message_date::date = d.full_date;
