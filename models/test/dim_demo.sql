{% set _path = model.path %}
{% set column_names = lst_bsn_cols(this) %}

{{ config(
    materialized='dim_new',
    file_format='delta', 
    strategy ='type2',
    identity_col = 'id',
    unique_key=['product_type'],
    catalog = 'fss',
    schema = 'ead_datamart'
) }}


with source as (

    select * 
    from {{source('test','demo')}} a

)
, source_rows as (
    select s.product_type, s.sales
        -------------technical columns-----------      
        , '{{ var("etl_date") }}'::date as tf_created_at
        , '{{ var("end_date") }}'::date as tf_updated_at
        , {{ dbt_utils.generate_surrogate_key( column_names )}} compare_key
     from source s
)
{% if is_incremental() %}
    , destination_rows as (
        Select t.* ,  {{ dbt_utils.generate_surrogate_key( column_names)}} compare_key
        from {{this}} t
        where tf_updated_at = '{{ var("end_date") }}'::date
    )
    , new_valid_to as (
        SELECT s.product_type, s.sales
                , s.tf_created_at
                , s.tf_updated_at
        FROM  source_rows s 
        left join destination_rows t 
        on s.product_type = t.product_type
        where nvl(s.compare_key, '$')  != nvl(t.compare_key, '$')    
    )
    , add_new_valid_to as (---End date ban ghi cu
        select    d.product_type
				, d.sales				
                , d.tf_created_at
                , '{{ var("etl_date") }}'::date as tf_updated_at  ---end date
          from destination_rows d
          join new_valid_to n
            on n.product_type = d.product_type 
    )
    , final as (
        SELECT * FROM new_valid_to
        UNION ALL 
        SELECT * FROM add_new_valid_to
    )

{% endif %}

select
    s.product_type, s.sales
    , s.tf_created_at
    , s.tf_updated_at
from source_rows s
{% if is_incremental() %}
where 1=2
union all 
select * from final;
{% endif %}


