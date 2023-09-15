{{ config(store_failures = true,
            schema = 'dbt_test__audit',
            pre_hook = 'drop table {{this}}') }}

with count_awm as
    (
        select 
            'card_txn_smy'::varchar as source, count (*) as count_row
        from {{ ref('card_txn_smy') }}
        where pymtc.snpst_dt <= '{{var("etl_date") }}'::date
    ),
count_dim as
    (
        select 'card_txn_dim'::varchar as source, count (*) as count_row
        from {{ ref('card_txn_dim') }}
        where '{{var("etl_date") }}'::date between cd.tf_created_at and cd.tf_updated_at 
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