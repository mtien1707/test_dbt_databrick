{{ config(store_failures = true,
            schema = 'dbt_test__audit',
            pre_hook = 'drop table {{this}}') }}

with awm as 
    (
        select
            card_dim.PYMTC_DIM_ID card_dim_id
            ,txn_dim.CARD_TXN_DIM_ID card_txn_dim_id
            ,ccy.CCY_DIM_ID txn_mini_dim_id
            ,cst_dim.CST_DIM_ID cst_dim_id
            ,card_txn.TXN_DT_TM txn_dt
            ,card_txn.TXN_AMT txn_amt
        from {{ ref('card_txn_smy') }} card_txn
            left join {{ ref('card_txn_smy') }} card_smy on card_txn.TRGT_ANCHOR_ID =  card_smy.CARD_ID
            left join {{ ref('cst_dim') }} cst_dim 
                on card_smy.CST_ID = cst_dim.CST_ID
                and cst_dim.TF_CREATED_AT <= '{{var("etl_date") }}'::date
                and cst_dim.TF_UPDATED_AT > '{{var("etl_date") }}'::date
            left join {{ ref('card_dim') }} card_dim
                on card_txn.TRGT_ANCHOR_ID = card_dim.CARD_ID
                and card_dim.TF_CREATED_AT <= '{{var("etl_date") }}'::date
                and card_dim.TF_UPDATED_AT > '{{var("etl_date") }}'::date
            left join {{ ref('card_txn_dim') }} txn_dim
                on card_txn.TXN_ANCHOR_ID = txn_dim.ANCHOR_ID
                and txn_dim.TF_CREATED_AT <= '{{var("etl_date") }}'::date
                and txn_dim.TF_UPDATED_AT > '{{var("etl_date") }}'::date
            left join {{ ref('txn_mini_dim') }} ccy_cnl
                on card_txn.TXN_CCY_CODE = ccy_cnl.ALPB_CCY_CODE
                and card_txn.TRGT_CNL_CODE = ccy_cnl.CNL_CODE
        where card_txn.TXN_DT_TM = '{{var("etl_date") }}'::date
            and card_smy.snpst_dt = '{{var("etl_date") }}'::date
    ),
dmt as
    (
        select
            card_dim_id
            ,card_txn_dim_id
            ,txn_mini_dim_id
            ,cst_dim_id
            ,txn_dt
            ,txn_amt
        from {{ ref('card_txn_fct') }} a
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