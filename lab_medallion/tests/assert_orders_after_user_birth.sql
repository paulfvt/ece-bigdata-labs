-- A user cannot order before being born.
-- Invalid birthdates are set to NULL in stg_users, so this test checks that every birthdate kept
-- in silver is before all the orders of the user. It fails if this query returns rows.
select
    o.order_id,
    o.ordered_at,
    u.user_id,
    u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u on o.user_id = u.user_id
where u.birthdate is not null
  and cast(o.ordered_at as date) < u.birthdate
