{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "ds",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


-- 1) load in impression table
-- 2) for each feature
--      a) cross join to impression table where feature is before engagement_ds
--      b) filter on most recent record

{% 
    set feature_tables = [
        'user_sparse_neighborhood_id',
        'object_sparse_restaurant_neighborhood_id'
    ]
%}

{% 
    set feature_names = ({
        'user_sparse_neighborhood_id':'user_neighborhood_id',
        'object_sparse_restaurant_neighborhood_id':'restaurant_neighborhood_id',
    })
%}


{% for feature in feature_tables %}
    {% if loop.first %}
    with {{ feature }}_staging as (    
    {%- else -%}
    ), {{ feature }}_staging as (
    {% endif %}
    
    select 
        * except(keep_col, feature_ds)
    from (
        select
            RANK() OVER (PARTITION BY engagement_id ORDER BY feature_ds DESC) as keep_col,
            *
        from (
            select 
                CURRENT_DATE('America/New_York') as ds,
                a.engagement_id,
                a.user_id,
                a.restaurant_id,
                a.engagement_ds,
                a.label,
                b.{{ feature_names[feature] }},
                b.ds as feature_ds
            from {{ ref('endpoint_save_given_impression') }} a
            left join {{ ref(feature) }} b
            {% if 'user' in feature %}
                on a.user_id = b.user_id and a.engagement_ds > b.ds
            {%- else -%}
                on a.restaurant_id = b.restaurant_id and a.engagement_ds > b.ds
            {% endif %}
            
        )
    ) where keep_col = 1

    {% if loop.last %}
    )
    {% endif %}
{% endfor %}

select
    a.*,
    [
    {% for feature in feature_tables %}
        STRUCT(
            '{{ feature_names[feature] }}' as feature,
            {{ feature }}_staging.{{ feature_names[feature] }} as value
        )
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    ] as features
from {{ ref('endpoint_save_given_impression') }} a
{% for feature in feature_tables %}
left join {{ feature }}_staging
on a.engagement_id = {{ feature }}_staging.engagement_id
{% endfor %}
