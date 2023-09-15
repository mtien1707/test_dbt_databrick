{% materialization dim_new, adapter='databricks' %}
    {{log('1111111111:'~ csv_colums, info=true)}}
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
        {%- set build_sql =   new_build_dim_initial_sql(target_relation, tmp_relation) %}
    {% else %}
        {%- set build_sql = new_build_dim_sql(target_relation, tmp_relation, unique_key, excludes_column_name, strategy) %}
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



{%- macro new_build_dim_initial_sql(target_relation, temp_relation) -%}
    {{ create_table_as(True, temp_relation, sql) }}
    {%- set initial_sql -%}
        SELECT
          * 
        FROM
          {{ temp_relation }}
    {%- endset -%}
    {{ create_table_as(False, target_relation, initial_sql) }}
{%- endmacro -%}


{%- macro new_build_dim_sql(target_relation, temp_relation, unique_key, excludes_column_name, strategy) -%}
    {{log('222222:'~ csv_colums, info=true)}}
    {%- set columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set unique_key = [] if unique_key is none else [] + unique_key -%}
    {#-- LyTTT: loai bo cot danh identity --#}
    {% set csv_colums = new_get_quoted_csv_fss(columns | map(attribute="name"), excludes_column_name) %}   

    {{log('csv_colums:'~ csv_colums, info=true)}}

    {%- set insert_cols_csv = new_get_quoted_csv_fss_alias(columns | map(attribute="name"), excludes_column_name)  -%}

    {{log('insert_cols_csv:'~ insert_cols_csv, info=true)}} 

    {{ create_table_as(True, temp_relation.identifier, compiled_code) }} 
    {#{{ create_view_as(temp_relation.identifier, sql)}}#}

    {% if strategy == 'type0' %}

        INSERT INTO {{ target_relation }} ({{ csv_colums }})
        SELECT DISTINCT
        * 
        FROM
        {{ temp_relation.table }}
        {#-- LyTTT: Them dieu kien NOT EXISTS theo unique_key #}
        WHERE NOT EXISTS( select 1 from {{ target_relation }} 
                        WHERE 
                        {% for key in unique_key %}
                        COALESCE({{ target_relation }}.{{ key }}, '$') = COALESCE({{ temp_relation.table }}.{{ key }}, '$')
                        {% if not loop.last %}
                        AND
                        {% endif %}
                        {% endfor %});
    {% else %} 
        {#-- strategy == 'type2'  ;    {{'t.'~column_names| join(',t.')}}  --#}
        {%- set predicates = [] -%}
        {% if unique_key %}
            {% if unique_key is not mapping and unique_key is not string %}
                {% for key in unique_key %}
                    {% set this_key_match %}
                        DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                    {% endset %}
                    {% do predicates.append(this_key_match) %}
                {% endfor %}
            {% else %}
                {% set unique_key_match %}
                    DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
                {% endset %}
                {% do predicates.append(unique_key_match) %}
            {% endif %}
        {% else %}
            {% do predicates.append('FALSE') %}
        {% endif %}

        merge into {{ target_relation }} as DBT_INTERNAL_DEST
        using {{ temp_relation.identifier }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

        when matched then update set
            tf_updated_at = DBT_INTERNAL_SOURCE.tf_updated_at           

        when not matched then insert ({{ csv_colums }})
                              values ({{ insert_cols_csv }}) 
                
    {% endif %}
    
{%- endmacro -%}

{%- macro new_get_quoted_csv_fss(column_names, excludes_column_name) -%}
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

{%- macro new_get_quoted_csv_fss_alias(column_names, excludes_column_name) -%}
    {%- set excludes_column_name = '' if excludes_column_name is none else excludes_column_name %}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- if col|upper != excludes_column_name|upper -%}
            {%- do quoted.append(adapter.quote('DBT_INTERNAL_SOURCE') ~'.' ~ adapter.quote(col)) -%}
        {%- endif -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{%- endmacro -%}

