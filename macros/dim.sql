{% materialization dim, adapter='databricks' %}

    {#-- get config #}
    {%- set target_relation = this %}
    {%- set unique_key = config.get('unique_key') %}
    {%- set excludes_column_name = config.get('identity_col', none) %}
    {#-- +++++ strategy : accepted values: type0 or type2, default type2 #}
    {%- set strategy = config.get('strategy', none) %} 

    {%- set tmp_identifier = "temp_" ~ target_relation.identifier %}

    {%- set tmp_relation = target_relation.incorporate(path={"identifier": tmp_identifier}) -%}
    {%- set existing_relation = load_relation(this) -%}
    {% if existing_relation is none or should_full_refresh() %}
        {%- set build_sql =   build_dim_initial_sql(target_relation, tmp_relation) %}
    {% else %}
        {%- set build_sql = build_dim_sql(target_relation, tmp_relation, unique_key, excludes_column_name, strategy) %}
    {% endif %}
{{- run_hooks(pre_hooks) -}}
    {%- call statement('main') -%}
        {{ build_sql }}
    {% endcall %}
    {{ run_hooks(post_hooks) }}
    {% do adapter.commit() %}
    {% set target_relation = this.incorporate(type='table') %}
    {% do persist_docs(target_relation, model) %}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}



{%- macro build_dim_initial_sql(target_relation, temp_relation) -%}
    {{ create_table_as(True, temp_relation, sql) }}
    {%- set initial_sql -%}
        SELECT
          * 
        FROM
          {{ temp_relation }}
    {%- endset -%}
    {{ create_table_as(False, target_relation, initial_sql) }}
{%- endmacro -%}


{%- macro build_dim_sql(target_relation, temp_relation, unique_key, excludes_column_name, strategy) -%}
    {%- set columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set unique_key = [] if unique_key is none else [] + unique_key -%}
    {#-- LyTTT: loai bo cot danh identity --#}
    {% set csv_colums = get_quoted_csv_fss(columns | map(attribute="name"), excludes_column_name) %}
    {{ create_temporary_view_as(temp_relation, sql) }}

    {% if strategy == 'type0' %}

        INSERT INTO {{ target_relation }} ({{ csv_colums }})
        SELECT DISTINCT
        * 
        FROM
        {# {{ temp_relation.table }} #}
        {{ temp_relation }}
        {#-- LyTTT: Them dieu kien NOT EXISTS theo unique_key #}
        WHERE NOT EXISTS( select 1 from {{ target_relation }} 
                        WHERE 
                        {% for key in unique_key %}
                        COALESCE({{ target_relation }}.{{ key }}, '$') = COALESCE({{ temp_relation }}.{{ key }}, '$')
                        {% if not loop.last %}
                        AND
                        {% endif %}
                        {% endfor %});
    {% else %} 
        {#-- strategy == 'type2'  ;    {{'t.'~column_names| join(',t.')}}  --#}
        {#--{%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%} --#}

        {#  /*
        {%- set str1 = unique_key|join(',')  -%}
        {%- set str2 = 'tmp.' ~ unique_key|join(', tmp.')  -%}
        */ #}

        {% set ns = namespace(strJoin = '') %}
        {% for col in unique_key %}
            {% set ns.strJoin = ns.strJoin ~ target_relation ~'.'~  col  ~ '=' ~ 'tmp.'  ~col  %}
            {%- if not loop.last %} 
                {% set ns.strJoin = ns.strJoin ~' and ' %}
            {% endif -%}            
        {% endfor %}   
        
        {% if unique_key is not none %}
            merge into {{ target }} as DBT_INTERNAL_DEST
            using {{ source }} as DBT_INTERNAL_SOURCE
            on FALSE

            when not matched by source
                {% if unique_key %} and {{ unique_key | join(' and ') }} {% endif %}
                then delete

            when not matched then insert
                ({{ csv_colums }})
            values
                ({{ csv_colums }})
        {% endif %}
        

        
    {% endif %}
    {{ drop_view(temp_relation) }}
{%- endmacro -%}

{%- macro get_quoted_csv_fss(column_names, excludes_column_name) -%}
    {%- set excludes_column_name = '' if excludes_column_name is none else excludes_column_name %}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- if col|upper != excludes_column_name|upper -%}
            {%- do quoted.append(adapter.quote(col)) -%}
        {%- endif -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{%- endmacro -%}


{%- macro create_temporary_view_as(temp_relation, sql) -%}
    {%- set parts = temp_relation -%}
    CREATE OR REPLACE VIEW {{ parts }} AS
    {{ sql }}
{%- endmacro -%}


{%- macro drop_view(temp_relation) -%}
    {%- set parts = temp_relation -%}
    DROP VIEW IF EXISTS {{ parts }};
{%- endmacro -%}

{# {%- macro create_temporary_view_as(temp_relation, sql) -%}
    {%- set parts = temp_relation.identifier.split('.') -%}
    CREATE OR REPLACE temporary  VIEW {{ parts[-1] }} AS
    {{ sql }}
{%- endmacro -%} #}


