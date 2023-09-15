{% macro lst_bsn_cols(target_relation, identity_col=none) %}    
    {%- set tech_cols = ['TF_CREATED_AT', 'TF_UPDATED_AT','PPN_DT','PPN_TM','JOB_NM'] -%}
    {%- if identity_col is not none -%}        
        {{- tech_cols.append(identity_col|upper) or "" -}}        
    {%-endif-%}
    {%- set final_tech_cols = tech_cols -%}
    
    {%- if is_incremental() %}
        {%- set columns = adapter.get_columns_in_relation(target_relation) -%}
        {%- set column_names = [] -%}
        {%- for column in columns -%}            
            {%- if column.name|upper not in final_tech_cols -%}
                {{- column_names.append(column.name) or "" -}}                
            {%- else -%}
                {{ log("exclude column: " ~ column.name, info=true) }}
            {%- endif -%}
        {%- endfor -%}    
        {{- log("Column: " ~ column_names, info=true) -}}
    {%- else -%}
        {%- set column_names = ['TF_CREATED_AT'] -%}
    {%- endif -%}
    {{return(column_names)}}  
{% endmacro %}