--{# {{ config(#}
--{#   schema='staging',#}
--{#   materialized='table',#}
--{#   tags = 'pst2',#}
--{#   properties = {#}
--{#     "format": "'PARQUET'",#}
--{#     "partitioning": "ARRAY['load_date_xml']",#}
--{#     "sorted_by": "ARRAY['rtl_txn_rk', 'load_ts_xml']"#}
--{#     }#}
--{# ) }}#}

{{ config(
  schema='staging_',
  materialized='incremental',
  incremental_strategy='append',
  tags = 'pst2'
) }}

select
    rtl_txn_rk,
    rtl_txn_disc_rk,
    retailstoreid,
    DATE(CAST(businessdaydate AS TIMESTAMP)) AS businessdaydate,
    SUBSTRING(fieldgroup, 1, 5) AS fieldgroup,
    SUBSTRING(fieldname, 1, 15) AS fieldname,
    fieldvalue,
    workstationid,
    transactionsequencenumber,
    CAST(discountsequencenumber AS INT) AS discountsequencenumber,
    transactiontypecode,
    load_ts,
    load_date_xml,
    load_ts_xml
FROM {{ source('raw_table', 'raw_bptransactdiscext') }}
where 1=1
{#    and {{ date_filter()  }}#}
{% if is_incremental() %}
    AND load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
    AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})
{% endif %}