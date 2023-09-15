{% set _path = model.path %}
{% set column_names = lst_bsn_cols(this) %}

{{ config(
    materialized='incremental',
    file_format='delta', 
    unique_key= ['card_txn_id', 'tf_created_at'],
    incremental_strategy='merge',
    catalog = 'fss',
    schema = 'ead_datamart'
) }}



with source as (
	select *
	from {{source('ead_atomic','card_txn_smy')}} txn
	where txn.TXN_DT_TM = '{{ var("etl_date") }}'::timestamp
)
, temp as (
    select 
        s.txn_anchor_id as card_txn_id
		,s.txn_dt_tm as eff_fm_dt
		,s.txn_dt_tm as eff_to_dt
		,s.src_stm as src_stm
		,s.txn_id as unq_id_in_src_stm
		,s.t24_txn_nbr as t24_txn_nbr
    from source s
)
, source_rows as (
    select a.card_txn_id
		, a.eff_fm_dt
		, a.eff_to_dt
        , a.src_stm
        , a.unq_id_in_src_stm
		, a.t24_txn_nbr
        -------------technical columns-----------      
        , {{dbt_date.today()}}::date ppn_dt
        , {{dbt_date.now()}}::timestamp::varchar(8) ppn_tm
        , '{{ var("etl_date") }}'::timestamp as tf_created_at
        , '{{ var("end_date") }}'::timestamp as tf_updated_at
        , {{ dbt_utils.generate_surrogate_key( column_names )}} compare_key
     from temp a
)

{% if is_incremental() %}
    , destination_rows as (---lay cac ban ghi co hieu luc o thoi diem hien tai    
        select *
               , {{ dbt_utils.generate_surrogate_key( column_names)}} compare_key
        from {{ this }} 
        where tf_updated_at = '{{ var("end_date") }}'::timestamp
    
    )
    , new_valid_to as (---xac dinh thong tin ban ghi moi duoc insert vao    
        select   s.card_txn_id
				, s.eff_fm_dt
				, s.eff_to_dt
				, s.src_stm
				, s.unq_id_in_src_stm
				, s.t24_txn_nbr
                , s.ppn_dt
                , s.ppn_tm
                , s.tf_created_at
                , s.tf_updated_at  -- thong tin la thong tin moi nhat tu Source, hieu luc tu ngay Etl -> oo                         
          from source_rows s
          left join destination_rows d
            on s.card_txn_id = d.card_txn_id 
         where nvl(s.compare_key, '$')  != nvl(d.compare_key, '$')      
    )
    , add_new_valid_to as (---End date ban ghi cu
        select    d.card_txn_id
				, d.eff_fm_dt
				, d.eff_to_dt
				, d.src_stm
				, d.unq_id_in_src_stm
				, d.t24_txn_nbr        
                , d.ppn_dt
                , d.ppn_tm
                , d.tf_created_at
                , '{{ var("etl_date") }}'::timestamp as tf_updated_at  ---end date
          from destination_rows d
          join new_valid_to n
            on n.card_txn_id = d.card_txn_id 
    )
    , final as (
        select 	  s.card_txn_id
				, s.eff_fm_dt
				, s.eff_to_dt
				, s.src_stm
				, s.unq_id_in_src_stm
				, s.t24_txn_nbr    
                , s.ppn_dt
                , s.ppn_tm
                , s.tf_created_at
                , s.tf_updated_at
                ,'{{_path}}' job_nm
        from add_new_valid_to s
        union
        select    d.card_txn_id
				, d.eff_fm_dt
				, d.eff_to_dt
				, d.src_stm
				, d.unq_id_in_src_stm
				, d.t24_txn_nbr             
                , d.ppn_dt
                , d.ppn_tm
                , d.tf_created_at
                , d.tf_updated_at 
                ,'{{_path}}' job_nm
                from new_valid_to d
    )
{% endif %}

select s.card_txn_id
	, s.eff_fm_dt
	, s.eff_to_dt
	, s.src_stm
	, s.unq_id_in_src_stm
	, s.t24_txn_nbr   
    , s.ppn_dt
    , s.ppn_tm
    , s.tf_created_at
    , s.tf_updated_at
    , '{{_path}}' job_nm
from source_rows s
{% if is_incremental() %}
    Where 1=2 
    union all 
    Select * from final
{% endif %}