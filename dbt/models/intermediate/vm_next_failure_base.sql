with next_failures as (

    select * from {{ ref('int_next_failure_hours') }}

),

final as (

    select
      machine_id,
      ts,
      
      case when hours_until_next_failure <= 24 then true else false end as failure_in_1day,
      case when hours_until_next_comp1_failure <= 24 then true else false end as comp1_failure_in_1day,
      case when hours_until_next_comp2_failure <= 24 then true else false end as comp2_failure_in_1day,
      case when hours_until_next_comp3_failure <= 24 then true else false end as comp3_failure_in_1day,
      case when hours_until_next_comp4_failure <= 24 then true else false end as comp4_failure_in_1day,

      case when hours_until_next_failure <= (24*7) then true else false end as failure_in_7day,
      case when hours_until_next_comp1_failure <= (24*7) then true else false end as comp1_failure_in_7day,
      case when hours_until_next_comp2_failure <= (24*7) then true else false end as comp2_failure_in_7day,
      case when hours_until_next_comp3_failure <= (24*7) then true else false end as comp3_failure_in_7day,
      case when hours_until_next_comp4_failure <= (24*7) then true else false end as comp4_failure_in_7day,

      case when hours_until_next_failure <= (24*30) then true else false end as failure_in_30day,
      case when hours_until_next_comp1_failure <= (24*30) then true else false end as comp1_failure_in_30day,
      case when hours_until_next_comp2_failure <= (24*30) then true else false end as comp2_failure_in_30day,
      case when hours_until_next_comp3_failure <= (24*30) then true else false end as comp3_failure_in_30day,
      case when hours_until_next_comp4_failure <= (24*30) then true else false end as comp4_failure_in_30day,

    from next_failures
    order by machine_id asc, ts desc

)

select * from final