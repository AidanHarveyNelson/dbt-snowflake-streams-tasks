select *
from {{ source('customers', 'testing') }}
