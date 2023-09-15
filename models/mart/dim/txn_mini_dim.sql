{{
    config(
        materialized = 'dim',
        unique_key = ['ccy_id','cnl_id'],
        identity_col = 'txn_mini_dim_id',
        strategy = 'type0',
        tags=['test'],
        catalog = 'fss',
        schema = 'ead_atomic'
    )
}}

with 
ccy_lov as(
    select a.cl_id, a.cl_code
    from {{source('ead_atomic','cl_val')}} a
    join {{source('ead_atomic','cl_scm')}} b on a.CL_SCM_ID = b.CL_SCM_ID
    where b.SCM_CODE = 'CCY-CODE'
),

cnl_lov as(
    select a.cl_id , a.cl_code
    from {{source('ead_atomic','cl_val')}} a
    join {{source('ead_atomic','cl_scm')}} b on a.CL_SCM_ID = b.CL_SCM_ID
    where b.SCM_CODE =  'WAY4-TXN-CNL'
)


SELECT
         ccy_lov.CL_CODE ALPB_CCY_CODE
        , ccy_lov.CL_ID CCY_ID
        , cnl_lov.CL_ID CNL_ID
        , cnl_lov.CL_CODE CNL_CODE
        ,  '{{ var("etl_date") }}'::timestamp as tf_created_at 
FROM ccy_lov
cross join cnl_lov;