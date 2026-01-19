-- stg_yolo_detections.sql
-- This staging model cleans and standardizes YOLO detection CSV data
-- Reads from raw.yolo_detections and prepares it for marts

with raw_detections as (

    select
        image_name,
        detected_objects,
        image_category,
        confidence_score::numeric as confidence_score
    from raw.yolo_detections

)

select
    image_name,
    detected_objects,
    image_category,
    confidence_score
from raw_detections
