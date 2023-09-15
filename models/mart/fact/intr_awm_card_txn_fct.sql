{#
{{
    config(
        materialized = 'incremental',
        unique_key=['card_dim_id', 'card_txn_dim_id','tf_created_at'],
        tags=['card_txn_fact'],
        catalog = 'fss',
        schema = 'ead_datamart',
        pre_hook = 'truncate table {{this}} '
    )
}}

with card_txn as(
    select * from {{ ref(card_txn_smy)}}
)

, card_smy as (
    select * from {{ ref(card_smy)}} card_smy
    where card_smy.snpst_dt = '{{ var("etl_date") }}'::date
)

, cst_dim as (
    select * from {{ ref(cst_dim)}} cst_dim
    where  '{{ var("etl_date") }}'::date between cst_dim.TF_CREATED_AT and cst_dim.TF_UPDATED_AT
)

, card_dim as (
    select * from {{ ref(card_dim)}} card_dim
    where '{{ var("etl_date") }}'::date between card_dim.TF_CREATED_AT and card_dim.TF_UPDATED_AT
)

, txn_dim as (
    select * from {{ ref(card_txn_dim)}} txn_dim
    where '{{ var("etl_date") }}'::date between txn_dim.TF_CREATED_AT and txn_dim.TF_UPDATED_AT
)

, ccy_cnl as (
    select * from {{ ref(txn_mini_dim)}}
)

,final as (
    select 
        card_dim.PYMTC_DIM_ID CARD_DIM_ID
        ,txn_dim.CARD_TXN_DIM_ID CARD_TXN_DIM_ID
        ,ccy_cnl.CCY_DIM_ID TXN_MINI_DIM_ID
        ,cst_dim.CST_DIM_ID CST_DIM_ID
        ,card_txn.TXN_DT_TM TXN_DT
        ,card_txn.TXN_AMT TXN_AMT
    from card_txn
        join card_smy 
            on card_txn.TRGT_ANCHOR_ID = card_smy.CARD_ID
        join cst_dim 
            on card_smy.CST_ID = cst_dim.CST_ID
        join card_dim
            on card_txn.TRGT_ANCHOR_ID = card_dim.CARD_ID
        join txn_dim
            on card_txn.TXN_ANCHOR_ID = txn_dim.ANCHOR_ID
        join ccy_cnl
            on card_txn.TXN_CCY_CODE = ccy_cnl.ALPB_CCY_CODE
            AND card_txn.TRGT_CNL_CODE = ccy_cnl.CNL_CODE
    where card_txn.TXN_DT_TM = '{{ var("etl_date") }}'::date
)
#}