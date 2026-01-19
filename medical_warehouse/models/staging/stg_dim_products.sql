-- models/staging/stg_dim_products.sql
-- Extract distinct products from YOLO detections
-- This model depends on YOLO detection data being available

with yolo_source as (
    -- Use stg_yolo_detections if available, otherwise return empty result
    select
        detected_objects,
        image_category as category
    from {{ ref('stg_yolo_detections') }}
    where detected_objects is not null
        and detected_objects != ''
        and detected_objects != '[]'
),

-- Split comma-separated detected objects into individual products
expanded_products as (
    select
        trim(unnest(string_to_array(detected_objects, ','))) as product_name,
        category
    from yolo_source
    where detected_objects is not null
)

select distinct
    row_number() over (order by product_name, category) as product_id,
    lower(trim(product_name)) as product_name,
    category
from expanded_products
where product_name is not null
    and trim(product_name) != ''
    and trim(product_name) != '[]'
order by product_id
