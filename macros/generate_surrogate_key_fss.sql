
{%- macro generate_surrogate_key_fss(arg_list) -%}
    {%- set default_null_value = "_dbt_utils_surrogate_key_null_" -%}
    {%- set fields = [] -%}
    
    {%- for arg in arg_list -%}
        {%- if arg.get('column') is none -%}
            {%- set text = arg.get('text') -%}
            {%- do fields.append(
                "'" ~ text ~ "'"
            ) -%}
        {%- else -%}
            {%- set column = arg.get('column') -%}
            {%- do fields.append(
                "coalesce(cast(" ~ column ~ " as TEXT), '" ~ default_null_value  ~ "')"
            ) -%}

        {%- endif -%}
    {%- endfor -%}

    {{ "md5(cast(" ~ fields|join(" || '-' || ") ~ " as TEXT))" }}
{%- endmacro %}


