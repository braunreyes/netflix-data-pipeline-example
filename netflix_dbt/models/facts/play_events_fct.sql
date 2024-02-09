
{{
    config(
        materialized='incremental'
    )
}}

select
    _partition_offset as partition_offset,
    json_query(_message, 'lax $.event_id') as event_id,
    json_query(_message, 'lax $.event_timestamp') as event_timestamp,
    json_query(_message, 'lax $.event_type') as event_type,
    json_query(_message, 'lax $.user_id') as user_id,
    json_query(_message, 'lax $.show_id') as show_id,
    _timestamp as message_timestamp

from {{ source('kafka', 'play_events') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where _partition_offset > (select max(partition_offset) from {{ this }})

{% endif %}
