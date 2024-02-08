{% snapshot users_dim %}

{{
    config(
      unique_key='user_id',
    )
}}

select
    user_id,
    user_name,
    date_of_birth,
    email,
    street_number,
    street_name,
    street_suffix,
    state,
    city,
    zip_code,
    added_on,
    updated_at
from {{ source('netflix', 'users_raw') }}
{% endsnapshot %}