select
    count(*) as total,

    count(case
        when status <> 'completed'
        or status is null
        then 1
    end) as dropped_status,

    count(case
        when status = 'completed'
        and (
            ended_at <= started_at
            or ended_at is null
            or started_at is null
        )
        then 1
    end) as dropped_time,

    count(case
        when status = 'completed'
        and ended_at > started_at
        then 1
    end) as kept

from {{ ref('stg_trips') }}
