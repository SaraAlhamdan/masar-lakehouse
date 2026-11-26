select count(*) as n
from {{ source('raw', 't') }}
