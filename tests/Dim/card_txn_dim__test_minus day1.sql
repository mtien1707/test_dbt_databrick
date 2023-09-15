{{ config(store_failures = true,
            schema = 'dbt_test__audit',
            pre_hook = 'drop table {{this}}') }}

with awm as 
    (
        select 
            txn.TXN_ANCHOR_ID as card_txn_id
            ,txn.TXN_DT_TM as eff_fm_dt
            ,txn.TXN_DT_TM as eff_to_dt
            ,txn.TF_SOURCE_SYSTEM as src_stm
            ,txn.TXN_ID as unq_id_in_src_stm
            ,txn.T24_TXN_NBR as t24_txn_nbr
        from {{ ref('card_txn_smy') }}
        where pymtc.snpst_dt <= '{{var("etl_date") }}'::date
    ),
dmt as
    (
        select
            card_txn_id
            ,eff_fm_dt
            ,eff_to_dt
            ,src_stm
            ,unq_id_in_src_stm
            ,t24_txn_nbr
        from {{ ref('card_txn_dim') }}
        where '{{var("etl_date") }}'::date between cd.tf_created_at and cd.tf_updated_at 
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