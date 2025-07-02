{% macro sat_record_start_date(order_by) %}
    {{ order_by }} as record_start_date
{% endmacro %}

{% macro sat_current_ind(partition_by, order_by) %}
    case 
        when lead({{ order_by }}) over (
            partition by {{ partition_by }}
            order by {{ order_by }}
        ) is null then true
        else false
    end as current_ind
{% endmacro %}

{% macro sat_version_number(partition_by, order_by) %}
    row_number() over (
        partition by {{ partition_by }}
        order by {{ order_by }}
    ) as version_number
{% endmacro %}

{% macro sat_reverse_version_number(partition_by, order_by) %}
    row_number() over (
        partition by {{ partition_by }}
        order by {{ order_by }} desc
    ) as reverse_version_number
{% endmacro %}

{% macro sat_record_end_date(partition_by, order_by) %}
    coalesce(
        dateadd(
            millisecond, -1,
            lead({{ order_by }}) over (
                partition by {{ partition_by }}
                order by {{ order_by }}
            )
        ),
        to_timestamp_ntz('8888-12-31 23:59:59.999')
    ) as record_end_date
{% endmacro %}