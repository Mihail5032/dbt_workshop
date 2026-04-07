SELECT
    xxhash64(
        CAST(CONCAT_WS('|',
	        retailstoreid,
	        businessdaydate,
	        workstationid,
	        transactionsequencenumber
	    ) AS VARBINARY)
	) AS rtl_txn_rk
	, xxhash64(
        CAST(CONCAT_WS('|',
	        retailstoreid,
	        businessdaydate,
	        workstationid,
	        transactionsequencenumber,
	        discountsequencenumber
	    ) AS VARBINARY)
	) AS rtl_txn_disc_rk
	, DATE(CAST(businessdaydate AS TIMESTAMP)) AS businessdaydate
	, discountsequencenumber
	, discounttypecode
	, reductionamount
	, retailstoreid
	, transactionsequencenumber
	, transactiontypecode
	, workstationid
	, load_ts
	, date(load_ts) as load_date
FROM lev_ice.raw.raw_table_part
WHERE segment_name = 'E1BPTRANSACTIONDISCO'