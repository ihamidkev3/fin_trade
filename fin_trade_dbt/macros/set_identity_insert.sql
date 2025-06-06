{% macro set_identity_insert(table, on_off) %}
{% set valid_values = ['ON', 'OFF'] %}

{% if on_off not in valid_values %}
    {% do exceptions.raise("Invalid value for `on_off`: " ~ on_off ~ ". Use 'ON' or 'OFF'.") %}
{% endif %}

{% set command = "SET IDENTITY_INSERT " ~ table ~ " " ~ on_off %}
{{ return(command) }}
{% endmacro %}