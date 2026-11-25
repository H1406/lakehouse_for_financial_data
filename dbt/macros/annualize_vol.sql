{# Annualized volatility = stdev of log returns over the window x sqrt(periods per year). See docs/METRICS.md. #}
{% macro annualize_vol(stddev_expr, periods_per_year=252) -%}
    ({{ stddev_expr }}) * sqrt({{ periods_per_year }})
{%- endmacro %}
