{% test failing(model, column_name) %}
select {{ column_name }} as value
from {{ model }}
where {{ column_name }} > 10 and {{ column_name }} < 20
{% endtest %}
