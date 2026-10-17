{% test positive(model, column_name) %}
-- Fails if a value is zero, negative or NULL (a quantity must be strictly positive)
select {{ column_name }}
from {{ model }}
where {{ column_name }} is null or {{ column_name }} <= 0
{% endtest %}
