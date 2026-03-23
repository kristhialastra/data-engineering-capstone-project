{% macro pct_of_total(numerator, denominator, decimals=2) %}
    ROUND(
        {{ numerator }}::NUMERIC / NULLIF({{ denominator }}, 0) * 100,
        {{ decimals }}
    )
{% endmacro %}
