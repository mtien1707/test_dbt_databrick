{{ config(store_failures = true,
            schema = 'dbt_test__audit',
            pre_hook = 'drop table {{this}}') }}

with ccy_lov as 
    (
        select 'stg_csv__cl_scm'::varchar as source, count(*) as count_row
        from {{ ref('cl_val') }} ccy_lov
            join {{ ref('cl_scm') }} ccy_scm on ccy_lov.CL_SCM_ID = ccy_scm.CL_SCM_ID
                and ccy_scm.SCM_CODE = 'CCY-CODE'
    ),
cnl_lov as 
    (
        select 'stg_csv__cl_scm'::varchar as source, count(*) as count_row
        from {{ ref('cl_val') }} cnl_lov
            join {{ ref('cl_scm') }} cnl_scm on cnl_lov.CL_SCM_ID = cnl_scm.CL_SCM_ID
                and cnl_scm.SCM_CODE = 'WAY4-TXN-CNL'
    ),
awm as 
    (
        select 
            ccy_lov.CL_CODE as alpb_ccy_code
            ,ccy_lov.CL_ID as ccy_id
            ,cnl_lov.CL_ID as cnl_id
            ,cnl_lov.CL_CODE as cnl_code
        from ccy_lov 
            cross join cnl_lov
    ),
dmt as
    (
        select
            alpb_ccy_code,
            ccy_id,
            cnl_id,
            cnl_code
        from {{ ref('cl_scm') }} a
    ),
check_minus as
    (
        select 'awm'::varchar as source, stg.* from awm
            minus
        select 'awm'::varchar as source, awm.* from dmt

        union all
        
        select 'dmt'::varchar as source, awm.* from dmt
            minus
        select 'dmt'::varchar as source, stg.* from awm
    )

select * from check_minus