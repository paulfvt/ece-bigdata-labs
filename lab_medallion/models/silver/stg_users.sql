with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        nullif(lower(trim(username)), '') as username_normalized,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate_raw,
        -- The address spans 2 lines: the street, then the city, the state and the zip code
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
),

-- First order of each user, to validate the birthdate
first_orders as (
    select user_id, cast(min(ordered_at) as date) as first_order_date
    from {{ ref('stg_orders') }}
    group by user_id
),

validated as (
    select
        t.*,
        -- Valid if born on or before the first order; a user with no order cannot be checked: kept as valid
        coalesce(t.birthdate_raw <= f.first_order_date, true) as birthdate_is_valid
    from typed t
    left join first_orders f on t.user_id = f.user_id
)

select
    user_id,
    username,
    username_normalized,
    name,
    sex,
    email,
    -- Invalid birthdates are replaced with NULL; the original value stays in birthdate_raw
    case when birthdate_is_valid then birthdate_raw end as birthdate,
    birthdate_raw,
    birthdate_is_valid,
    street,
    -- Military addresses, such as "DPO AE 12345", have no comma
    nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from validated
