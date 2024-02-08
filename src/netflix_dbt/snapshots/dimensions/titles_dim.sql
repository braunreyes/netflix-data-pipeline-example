{% snapshot titles_dim %}

{{
    config(
      unique_key='show_id',
    )
}}

select
    show_id,
    "type" as show_type,
    title,
    director,
    "cast" as show_cast,
    country,
    date_added,
    release_year,
    rating,
    duration,
    listed_in,
    "description" as show_description,
    updated_at
from {{ source('netflix', 'titles_raw') }}
{% endsnapshot %}