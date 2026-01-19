with source as (

    select
        image_name,
        detected_objects,
        image_category,
        confidence_score
    from raw.yolo_detections

),

cleaned as (

    select
        -- image identifier
        image_name::text as image_name,

        -- detected objects as text (comma-separated)
        nullif(detected_objects, '')::text as detected_objects,

        -- standardize category values
        lower(image_category)::text as image_category,

        -- ensure numeric confidence
        confidence_score::numeric as confidence_score

    from source
)

select *
from cleaned
where image_name is not null
