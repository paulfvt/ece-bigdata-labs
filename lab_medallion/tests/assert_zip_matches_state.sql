{{ config(severity='warn') }}

-- The 3 first digits of a US ZIP code depend on the state.
-- Returns the users whose zip_code is not in a valid range of their state.
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('zip_prefix_ranges') }} r
    where r.state = u.state
      and cast(left(u.zip_code, 3) as integer) between r.prefix_min and r.prefix_max
)
