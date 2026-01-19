-- models/marts/mart_top_products.sql
-- This creates a "top products" mart using messages as the product equivalent

with message_stats as (
    select
        m.message_id as product_id,
        m.message_text as product_name,
        c.channel_key as category,
        m.view_count as units_sold,       -- using views as "sales"
        m.forward_count as total_sales,   -- using forwards as "revenue" equivalent
        m.has_image,
        m.image_name,
        d.full_date as last_sold_date
    from {{ ref('fct_messages') }} m
    left join {{ ref('dim_channels') }} c
        on m.channel_key = c.channel_key
    left join {{ ref('dim_dates') }} d
        on m.date_key = d.full_date
),

ranked_products as (
    select
        *,
        row_number() over (partition by category order by units_sold desc) as rank
    from message_stats
)

select *
from ranked_products
where rank <= 10
