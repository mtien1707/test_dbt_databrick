{#
{% set _path = model.path %}
{% set column_names = lst_bsn_cols(this) %}
{{
    config(
        materialized = 'incremental',
        pre_hook = 'delete from {{this}} where TXN_DT = {{ var("etl_date")::date }}',
        tags = ["fact", "card_txn_fct"]
    )
}}

with final as (
    select * from {{ ref('intr_mart_card_ar_fct__card_ar') }} 
)
select 
    CARD_DIM_ID
    ,CARD_TXN_DIM_ID
    ,TXN_MINI_DIM_ID
    ,CST_DIM_ID
    ,TXN_DT
    ,TXN_AMT
    ,{{dbt_date.today()}} ppn_dt
    ,{{dbt_date.now()}}::time ppn_tm
    ,'{{ _path }}' job_nm
from final s
#}