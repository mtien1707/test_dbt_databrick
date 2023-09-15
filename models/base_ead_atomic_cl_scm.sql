with source as (
      select * from {{ source('ead_atomic', 'cl_scm') }}
),
renamed as (
    select
        {{ adapter.quote("cl_scm_id") }},
        {{ adapter.quote("src_stm_id") }},
        {{ adapter.quote("scm_code") }},
        {{ adapter.quote("shrt_nm") }},
        {{ adapter.quote("tf_created_at") }},
        {{ adapter.quote("tf_updated_at") }},
        {{ adapter.quote("ppn_dt") }},
        {{ adapter.quote("ppn_tm") }},
        {{ adapter.quote("job_nm") }}

    from source
)
select * from renamed
  