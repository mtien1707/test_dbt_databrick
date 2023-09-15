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
count_awm as
    (
        select 
            'cl_val'::varchar as source, count (*) as count_row
        from ccy_lov 
            cross join cnl_lov
    ),
count_dim as
    (
        select 'txn_mini_dim'::varchar as source, count (*) as count_row
        from {{ ref('txn_mini_dim') }}
    ),
check_count as
    (
        select
            {{dbt_date.today()}} ppn_dt,
            'Test_count'::varchar as Test_type,
            a.source as source_tbl,
            b.source as target_tbl,
            a.count_row as count_source,
            b.count_row as count_target
        from count_awm a 
        full join count_dim b on 1=1
    )

    select 
        *
    from check_count where count_source <> count_target