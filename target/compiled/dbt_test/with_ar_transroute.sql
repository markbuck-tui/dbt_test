

SELECT
	tr.trans_route_id
	,tr.route_cd
FROM opa_stg_uk.ar_transroute tr
WHERE tr.file_dt = (SELECT MAX(tr_2.file_dt) FROM opa_stg_uk.ar_transroute tr_2 WHERE tr.trans_route_id = tr_2.trans_route_id)