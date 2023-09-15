{{ config(
    materialized='incremental',
    file_format='delta', 
    unique_key='cl_scm_id',
    incremental_strategy='merge',
    catalog = 'fss',
    schema = 'ead_atomic'
) }}

with source as (

    select * from {{source('ead_atomic','cl_val')}} a

)
{% if is_incremental() %}
    , targets as (
        Select * from {{this}}
    )
    , compare_tbl as (
        SELECT s.cl_scm_id
                ,s.src_stm_id
                ,s.scm_code
                ,s.shrt_nm
        FROM  source s 
        left join targets t 
        on s.cl_scm_id = t.cl_scm_id
        where nvl(s.scm_code, '$') <> nvl(t.scm_code, '$') 
           or nvl(s.shrt_nm, '$') <> nvl(t.shrt_nm, '$') 
    )

{% endif %}

select
    cl_scm_id
    ,src_stm_id
    ,scm_code
    ,shrt_nm
from source
{% if is_incremental() %}
where 1=2
union all 
select * from compare_tbl
{% endif %}
