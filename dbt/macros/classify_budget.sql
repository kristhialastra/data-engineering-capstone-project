{% macro classify_budget(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL      THEN 'Unknown'
        WHEN {{ column_name }} >= 100000000 THEN 'Blockbuster'   -- $100M+
        WHEN {{ column_name }} >= 20000000  THEN 'Mid-Range'     -- $20M–$99M
        WHEN {{ column_name }} >= 1000000   THEN 'Low-Budget'    -- $1M–$19M
        ELSE                                     'Micro-Budget'  -- Under $1M
    END
{% endmacro %}
