{{
  config(materialized='view')
}}

WITH fl_acr_booking AS (
	SELECT
		bk_1.sk_booking_id
		,bk_1.booking_version
		,bk_1.atcom_res_id
		,bk_1.atcom_res_version
          ,bk_1.atcom_market_id -- CG ADDED in V1.06 for new Source Market Derivation
		,bk_1.number_of_adults
		,bk_1.number_of_children
		,bk_1.number_of_infants
		,bk_1.number_of_passengers
		,bk_1.sk_season_id
		,bk_1.booking_status
		,bk_1.atcom_agent_id
		,bk_1.atcom_sell_currency_id
		,bk_1.season_date
		,bk_1.confirmed_on
		,bk_1.cancelled_on
		,bk_1.source_created_on
		,bk_1.modified_on
		,bk_1.dwh_created_on
		,bk_1.dwh_modified_on

	FROM opa_stg_uk.fl_acr_booking bk_1
	WHERE bk_1.file_dt = (SELECT MAX(bk_2.file_dt) FROM opa_stg_uk.fl_acr_booking bk_2 WHERE bk_1.sk_booking_id = bk_2.sk_booking_id AND bk_1.booking_version = bk_2.booking_version)
	AND bk_1.booking_version = (SELECT MAX(bk_3.booking_version) FROM opa_stg_uk.fl_acr_booking bk_3 WHERE bk_1.sk_booking_id = bk_3.sk_booking_id)
	AND (bk_1.sk_season_id > 201701 OR bk_1.sk_booking_id IS NULL)

	-- To be removed when running against all bookings
	AND bk_1.sk_booking_id IN ('380402','975528','10016009','10063844','15994298','22568921','25059884','27813713','28536240','30846203','33404409','20348866','31280892','35353771')
)
,ar_sellstatic AS (
	SELECT
		sls_1.sell_stc_id
		,sls_1.stc_stk_id
	FROM opa_stg_uk.ar_sellstatic sls_1
	WHERE sls_1.file_dt = (SELECT MAX(sls_2.file_dt) FROM opa_stg_uk.ar_sellstatic sls_2 WHERE sls_1.sell_stc_id = sls_2.sell_stc_id)
)
,ar_staticstock AS (
	SELECT
		ss.stc_stk_id
		,ss.cd
	FROM opa_stg_uk.ar_staticstock ss
	WHERE ss.file_dt = (SELECT MAX(ss_2.file_dt) FROM opa_stg_uk.ar_staticstock ss_2 WHERE ss.stc_stk_id = ss_2.stc_stk_id)
)
,ar_staticroom AS (
  SELECT
	sr.stc_rm_id
	,sr.stc_stk_id
	,sr.rm_id
  FROM opa_stg_uk.ar_staticroom sr
  WHERE sr.file_dt = (SELECT MAX(sr_2.file_dt) FROM opa_stg_uk.ar_staticroom sr_2 WHERE sr.stc_rm_id = sr_2.stc_rm_id)
)
,ar_sellunit AS (
	SELECT
		su.sell_unit_id
		,su.rm_id
		,su.bb_cd_id
	FROM opa_stg_uk.ar_sellunit su
	WHERE su.file_dt = (SELECT MAX(su_2.file_dt) FROM opa_stg_uk.ar_sellunit su_2 WHERE su.sell_unit_id = su_2.sell_unit_id)
)
,ar_usercodes AS (
	SELECT DISTINCT
		uc_1.user_cd_id
		,uc_1.cd
		,uc_1.name
	FROM opa_stg_uk.ar_usercodes uc_1
	WHERE uc_1.file_dt = (SELECT MAX(uc_2.file_dt) FROM opa_stg_uk.ar_usercodes uc_2 WHERE uc_1.user_cd_id = uc_2.user_cd_id)
)
,fl_acr_booking_service AS (
	SELECT
		bk_ser_1.sk_booking_service_id
		,bk_ser_1.sk_booking_id
		,bk_ser_1.sk_service_id
		,bk_ser_1.service_version
		,bk_ser_1.booking_version
	FROM opa_stg_uk.fl_acr_booking_service bk_ser_1
	WHERE bk_ser_1.file_dt = (SELECT MAX(bk_ser_2.file_dt) FROM opa_stg_uk.fl_acr_booking_service bk_ser_2 WHERE bk_ser_1.sk_booking_service_id = bk_ser_2.sk_booking_service_id)
	AND bk_ser_1.booking_version = (SELECT MAX(bk_ser_3.booking_version) FROM opa_stg_uk.fl_acr_booking_service bk_ser_3 WHERE bk_ser_1.sk_booking_id = bk_ser_3.sk_booking_id)
	AND bk_ser_1.service_version = (SELECT MAX(bk_ser_4.service_version) FROM opa_stg_uk.fl_acr_booking_service bk_ser_4 WHERE bk_ser_1.sk_service_id = bk_ser_4.sk_service_id)

	-- To be removed when running against all bookings
	AND bk_ser_1.sk_booking_id IN ('380402','975528','10016009','10063844','15994298','22568921','25059884','27813713','28536240','30846203','33404409','20348866','31280892','35353771')
)
,fl_acr_service AS (
	SELECT
		ser_1.sk_service_id
		,ser_1.atcom_ser_id
		,ser_1.atcom_dep_point_id
		,ser_1.service_version
		,ser_1.service_status
		,ser_1.direction
		,ser_1.sell_type
		,ser_1.service_type
		,ser_1.flight_type_code
		,ser_1.service_start_date1
		,ser_1.service_end_date1
		,ser_1.departure_flight_number
		,ser_1.atcom_arr_point_id
		,ser_1.source_stock_type_code
	FROM opa_stg_uk.fl_acr_service ser_1
	WHERE ser_1.file_dt = (SELECT MAX(ser_2.file_dt) FROM opa_stg_uk.fl_acr_service ser_2 WHERE ser_1.sk_service_id = ser_2.sk_service_id AND ser_1.service_version = ser_2.service_version)
)
,fl_acr_service_element AS (
	SELECT
		ser_e_1.sk_service_id
		,ser_e_1.service_version
		,ser_e_1.atcom_sub_ser_id
	FROM opa_stg_uk.fl_acr_service_element ser_e_1
	WHERE ser_e_1.file_dt = (SELECT MAX(ser_e_2.file_dt) FROM opa_stg_uk.fl_acr_service_element ser_e_2 WHERE ser_e_1.sk_service_element_id = ser_e_2.sk_service_element_id)
)
,ar_transroute AS (
	SELECT
		tr.trans_route_id
		,tr.route_cd
	FROM opa_stg_uk.ar_transroute tr
	WHERE tr.file_dt = (SELECT MAX(tr_2.file_dt) FROM opa_stg_uk.ar_transroute tr_2 WHERE tr.trans_route_id = tr_2.trans_route_id)
)
,ar_transinvroute AS (
	SELECT
		tir.trans_inv_route_id
		,tir.trans_route_id
	FROM opa_stg_uk.ar_transinvroute tir
	WHERE tir.file_dt = (SELECT MAX(tir_2.file_dt) FROM opa_stg_uk.ar_transinvroute tir_2 WHERE tir.trans_inv_route_id = tir_2.trans_inv_route_id)
)
,ar_transinvroutesector AS (
	SELECT
		tirs.trans_inv_route_id
		,tirs.trans_inv_sec_id
		,tirs.trans_inv_route_sec_id
	FROM opa_stg_uk.ar_transinvroutesector tirs
	WHERE tirs.file_dt = (SELECT MAX(tirs_2.file_dt) FROM opa_stg_uk.ar_transinvroutesector tirs_2 WHERE tirs.trans_inv_route_sec_id = tirs_2.trans_inv_route_sec_id)
),
ar_transinvsector AS (
	SELECT
		tis.trans_inv_sec_id
		,tis.dep_dt_tm
	FROM opa_stg_uk.ar_transinvsector tis
	WHERE tis.file_dt = (SELECT MAX(tis_2.file_dt) FROM opa_stg_uk.ar_transinvsector tis_2 WHERE tis.trans_inv_sec_id = tis_2.trans_inv_sec_id)
)
,ar_point AS (
	SELECT
		p.pt_id
		,p.pt_cd
	FROM opa_stg_uk.ar_point p
	WHERE p.file_dt = (SELECT MAX(p_2.file_dt) FROM opa_stg_uk.ar_point p_2 WHERE p.pt_id = p_2.pt_id)
)
,dates AS (
	SELECT
		dd.bk_date
		,dd.group_season_code
	FROM opa_stg_all.date_dim dd
)
,booking_service AS (
	SELECT
		bk_ser.sk_booking_id
		,bk_ser.booking_version
		,bk_ser.sk_service_id
		,bk_ser.service_version
		,bk_ser.sk_booking_service_id
		,ser.service_type
		,ser.source_stock_type_code
		,ser.sell_type
		,ser.service_status
		,ser.flight_type_code
		,ser.service_start_date1
		,ser.service_end_date1
		,tis.dep_dt_tm
		,ser.departure_flight_number
		,ser.direction
		,tr.route_cd
		,dpt.pt_cd
		,CASE WHEN
			-- All flight cancelled
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			CASE WHEN
				ser.service_start_date1 =
					MIN(CASE WHEN
							(ser.service_type = 'TRS' AND ser.sell_type = 'FLT')
							OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT')
						THEN ser.service_start_date1
						ELSE NULL
						END
					) OVER (PARTITION BY bk_ser.sk_booking_id)
				THEN apt.pt_cd
				ELSE NULL
			END
		ELSE
			CASE WHEN
				ser.service_start_date1 =
					MIN(CASE WHEN
							(ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
							OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
						THEN ser.service_start_date1
						ELSE NULL
						END
					) OVER (PARTITION BY bk_ser.sk_booking_id)
				AND ser.service_status = 'CON'
				THEN apt.pt_cd
				ELSE NULL
			END
		END AS min_flight_gateway

		,CASE WHEN
			-- All flight cancelled
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			CASE WHEN
				ser.service_start_date1 =
					MIN(CASE WHEN
							(ser.service_type = 'TRS' AND ser.sell_type = 'FLT')
							OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT')
						THEN ser.service_start_date1
						ELSE NULL
						END
					) OVER (PARTITION BY bk_ser.sk_booking_id)
				THEN
					CASE WHEN tr.route_cd IS NULL
						THEN
							ser.departure_flight_number|| '|' || dpt.pt_cd || '|' || apt.pt_cd|| '|' || CAST(SUBSTR(ser.service_start_date1,1,4)||SUBSTR(ser.service_start_date1,6,2)||SUBSTR(ser.service_start_date1,9,2) AS VARCHAR)
						ELSE
							tr.route_cd || '|' || SUBSTRING(tis.dep_dt_tm, 1, 4) || SUBSTRING(tis.dep_dt_tm, 6, 2) || SUBSTRING(tis.dep_dt_tm, 9, 2)
						END
				ELSE NULL
			END
		ELSE
			CASE WHEN
				ser.service_start_date1 =
					MIN(CASE WHEN
							(ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
							OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
						THEN ser.service_start_date1
						ELSE NULL
						END
					) OVER (PARTITION BY bk_ser.sk_booking_id)
					AND ser.service_status = 'CON'
				THEN
					CASE WHEN tr.route_cd IS NULL
						THEN
							ser.departure_flight_number|| '|' || dpt.pt_cd || '|' || apt.pt_cd|| '|' || CAST(SUBSTR(ser.service_start_date1,1,4)||SUBSTR(ser.service_start_date1,6,2)||SUBSTR(ser.service_start_date1,9,2) AS VARCHAR)
						ELSE
							tr.route_cd || '|' || SUBSTRING(tis.dep_dt_tm, 1, 4) || SUBSTRING(tis.dep_dt_tm, 6, 2) || SUBSTRING(tis.dep_dt_tm, 9, 2)
						END
				ELSE NULL
			END
		END	AS min_flight_id

		,CASE WHEN
			-- All flight cancelled
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			CASE WHEN
				ser.service_start_date1 =
					MAX(CASE WHEN
							(ser.service_type = 'TRS' AND ser.sell_type = 'FLT')
							OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT')
						THEN ser.service_start_date1
						ELSE NULL
						END
					) OVER (PARTITION BY bk_ser.sk_booking_id)
				THEN
					CASE WHEN tr.route_cd IS NULL
						THEN
							ser.departure_flight_number|| '|' || dpt.pt_cd || '|' || apt.pt_cd|| '|' || CAST(SUBSTR(ser.service_start_date1,1,4)||SUBSTR(ser.service_start_date1,6,2)||SUBSTR(ser.service_start_date1,9,2) AS VARCHAR)
						ELSE
							tr.route_cd || '|' || SUBSTRING(tis.dep_dt_tm, 1, 4) || SUBSTRING(tis.dep_dt_tm, 6, 2) || SUBSTRING(tis.dep_dt_tm, 9, 2)
						END
				ELSE NULL
			END
		ELSE
			CASE WHEN
				ser.service_start_date1 =
					MAX(CASE WHEN
							(ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
							OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
						THEN ser.service_start_date1
						ELSE NULL
						END
					) OVER (PARTITION BY bk_ser.sk_booking_id)
					AND ser.service_status = 'CON'
				THEN
					CASE WHEN tr.route_cd IS NULL
						THEN
							ser.departure_flight_number|| '|' || dpt.pt_cd || '|' || apt.pt_cd|| '|' || CAST(SUBSTR(ser.service_start_date1,1,4)||SUBSTR(ser.service_start_date1,6,2)||SUBSTR(ser.service_start_date1,9,2) AS VARCHAR)
						ELSE
							tr.route_cd || '|' || SUBSTRING(tis.dep_dt_tm, 1, 4) || SUBSTRING(tis.dep_dt_tm, 6, 2) || SUBSTRING(tis.dep_dt_tm, 9, 2)
						END
				ELSE NULL
			END
		END AS max_flight_id


		-- MULTICENTRE
		,CASE WHEN
			-- All multicentre services cancelled
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MIN(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM')
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MIN(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC' AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CON')
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END	AS min_multi_center_date

		,CASE WHEN
			-- All multicentre services cancelled
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MAX(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM')
				THEN ser.service_end_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MAX(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'MC' AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CON')
				THEN ser.service_end_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END AS max_multi_center_date


		-- ACCOM
		,CASE WHEN
			-- All accom services cancelled
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MIN(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU'))
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM')
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MIN(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CON')
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END AS min_accom_date
		,CASE WHEN
			-- All accom services cancelled
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MAX(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU'))
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM')
				THEN ser.service_end_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MAX(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'ACCM' AND ser.service_status = 'CON')
				THEN ser.service_end_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END AS max_accom_date


		-- CRUISE
		,CASE WHEN
			-- All accom services cancelled
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'CRU' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'CRU' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MIN(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'CRU')
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MIN(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'CRU' AND ser.service_status = 'CON')
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END AS min_cruise_date
		,CASE WHEN
			-- All accom services cancelled
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'CRU' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'CRU' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MAX(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'CRU')
				THEN ser.service_end_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MAX(CASE WHEN
					(ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'CRU' AND ser.service_status = 'CON')
				THEN ser.service_end_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END max_cruise_date


		-- FLIGHT
		,CASE WHEN
			-- All flight services cancelled
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MIN(CASE WHEN
					(ser.service_type = 'TRS' AND ser.sell_type = 'FLT')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT')
				--THEN tis.dep_dt_tm END)
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MIN(CASE WHEN
					(ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
				--THEN tis.dep_dt_tm END)
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END AS min_flight_date
		,CASE WHEN
			-- All flight services cancelled
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			=
			SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
		THEN
			MAX(CASE WHEN
					(ser.service_type = 'TRS' AND ser.sell_type = 'FLT')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT')
				--THEN tis.dep_dt_tm END)
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		ELSE
			MAX(CASE WHEN
					(ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
					OR (ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.service_status = 'CON')
				--THEN tis.dep_dt_tm END)
				THEN ser.service_start_date1 END)
			OVER (PARTITION BY bk_ser.sk_booking_id)
		END	AS max_flight_date

		,sts.cd AS accom
		,su.bb_cd_id AS board_cd
		,uc_3.name AS board_name
		,str.stc_rm_id AS room

		-- Booking type derivation part 1
		,CASE WHEN
				-- No outbound flight services on the booking are third party
				SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) = 0
		THEN 0
		ELSE
			CASE WHEN
				-- All outbound flight services are 3PF
				SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
					+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				=
				SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)

			OR
				SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'OUT' AND ser.service_status = 'CON' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) > 1
			THEN 1
			ELSE 0
			END
		END AS tpf_out_count
		,CASE WHEN
				-- No outbound flight services on the booking are third party
				SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) = 0
		THEN 0
		ELSE
			CASE WHEN
				-- All outbound flight services are 3PF
				SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
					+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				=
				SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)

			OR
				SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'IN' AND ser.service_status = 'CON' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) > 1
			THEN 1
			ELSE 0
			END
		END AS tpf_in_count
		,CASE WHEN
				CASE WHEN
						-- No outbound flight services on the booking are third party
						SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) = 0
				THEN 0
				ELSE
					CASE WHEN
						-- All outbound flight services are 3PF
						SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
							+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
						=
						SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)

					OR
						SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'OUT' AND ser.service_status = 'CON' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) > 1
					THEN 1
					ELSE 0
					END
				END
			+
				CASE WHEN
						-- No outbound flight services on the booking are third party
						SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) = 0
				THEN 0
				ELSE
					CASE WHEN
						-- All outbound flight services are 3PF
						SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
							+ SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
						=
						SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)

					OR
						SUM(CASE WHEN ser.flight_type_code = 'T' AND ser.direction = 'IN' AND ser.service_status = 'CON' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id) > 1
					THEN 1
					ELSE 0
					END
				END
		> 0
			THEN 'Y'
			ELSE 'N'
		END AS tpf_indicator
		,CASE WHEN
				-- All accom cancelled
				SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				= SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			THEN CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') THEN 1 ELSE 0 END
			ELSE CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code NOT IN ('MC', 'CRU') AND ser.service_status = 'CON' THEN 1 ELSE 0 END
		END AS accom_count
		,CASE WHEN
				-- All cruise cancelled
				SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				= SUM(CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			THEN CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' THEN 1 ELSE 0 END
			ELSE CASE WHEN ser.service_type = 'ACC' AND ser.source_stock_type_code = 'CRU' AND ser.service_status = 'CON' THEN 1 ELSE 0 END
		END AS cruise_count
		,CASE WHEN
				-- All flight out cancelled
				SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				= SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			THEN CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN 1 ELSE 0 END
			ELSE CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' AND ser.service_status = 'CON' THEN 1 ELSE 0 END
		END AS flight_out_count
		,CASE WHEN
				-- All flight in cancelled
				SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				= SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			THEN CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN 1 ELSE 0 END
			ELSE CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' AND ser.service_status = 'CON' THEN 1 ELSE 0 END
		END AS flight_ret_count
		,CASE WHEN
				-- All ahoc services cancelled
				SUM(CASE WHEN ser.service_type = 'AHOC' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				= SUM(CASE WHEN ser.service_type = 'AHOC' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			THEN CASE WHEN ser.service_type = 'AHOC' THEN 1 ELSE 0 END
			ELSE CASE WHEN ser.service_type = 'AHOC' AND ser.service_status = 'CON' THEN 1 ELSE 0 END
		END AS thirdparty_count

		,CASE WHEN
				-- All flight out first date cancelled
				SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				= SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			THEN CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' THEN ser.service_start_date1 ELSE NULL END
			ELSE CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'OUT' AND ser.service_status = 'CON' THEN ser.service_start_date1 ELSE NULL END
		END AS flight_out_first_date
		,CASE WHEN
				-- All flight in first date cancelled
				SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
				= SUM(CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' AND ser.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bk_ser.sk_booking_id)
			THEN CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' THEN ser.service_start_date1 ELSE NULL END
			ELSE CASE WHEN ser.service_type = 'TRS' AND ser.sell_type = 'FLT' AND ser.direction = 'IN' AND ser.service_status = 'CON' THEN ser.service_start_date1 ELSE NULL END
		END AS flight_ret_first_date

	-- Service and subservice
	FROM fl_acr_booking_service bk_ser
	LEFT OUTER JOIN fl_acr_service ser ON bk_ser.sk_service_id = ser.sk_service_id AND bk_ser.service_version = ser.service_version
	LEFT OUTER JOIN fl_acr_service_element ser_e ON ser.sk_service_id = ser_e.sk_service_id AND ser.service_version = ser_e.service_version

	-- Accom
	LEFT OUTER JOIN ar_sellstatic sls ON ser.atcom_ser_id = sls.sell_stc_id
	LEFT OUTER JOIN ar_staticstock sts ON sls.stc_stk_id = sts.stc_stk_id

	-- Room
	LEFT OUTER JOIN ar_sellunit su ON ser_e.atcom_sub_ser_id = su.sell_unit_id
	LEFT OUTER JOIN ar_staticroom str ON sts.stc_stk_id = str.stc_stk_id AND su.rm_id = str.rm_id

	-- Board
	LEFT OUTER JOIN ar_usercodes uc_3 ON su.bb_cd_id = uc_3.user_cd_id

	-- Flight
	LEFT OUTER JOIN ar_transinvroute tir ON ser.atcom_ser_id = tir.trans_inv_route_id
	LEFT OUTER JOIN ar_transroute tr ON tir.trans_route_id = tr.trans_route_id
	LEFT OUTER JOIN ar_transinvroutesector tirs ON tir.trans_inv_route_id = tirs.trans_inv_route_id
      LEFT OUTER JOIN ar_transinvsector tis ON tirs.trans_inv_sec_id = tis.trans_inv_sec_id
	LEFT OUTER JOIN ar_point dpt ON ser.atcom_dep_point_id = dpt.pt_id
	LEFT OUTER JOIN ar_point apt ON ser.atcom_arr_point_id = apt.pt_id

	ORDER BY
		bk_ser.sk_booking_id
		,bk_ser.booking_version
		,ser.source_stock_type_code

)
,ar_agent AS (
	SELECT DISTINCT
		ag_1.agt_id
		,ag_1.def_mkt_id
		,ag_1.agt_tp_id
	FROM opa_stg_uk.ar_agent ag_1
	WHERE ag_1.file_dt = (SELECT MAX(ag_2.file_dt) FROM opa_stg_uk.ar_agent ag_2 WHERE ag_1.agt_id = ag_2.agt_id)
)
,ar_market AS (
	SELECT DISTINCT
		mk_1.mkt_id
		,mk_1.off_id
	FROM opa_stg_uk.ar_market mk_1
	WHERE mk_1.file_dt = (SELECT MAX(mk_2.file_dt) FROM opa_stg_uk.ar_market mk_2 WHERE mk_1.mkt_id = mk_2.mkt_id)
)
,ar_officename AS (
	SELECT DISTINCT
		ofn_1.off_name_id
		,ofn_1.cd
		,ofn_1.name
		-- ,sm.source_market_code
	FROM opa_stg_uk.ar_officename ofn_1
	-- LEFT OUTER JOIN opa_stg_all.source_market sm ON 'UKATCOM|' || ofn_1.cd = bk_source_market
	WHERE ofn_1.file_dt = (SELECT MAX(ofn_2.file_dt) FROM opa_stg_uk.ar_officename ofn_2 WHERE ofn_1.off_name_id = ofn_2.off_name_id)
)
,ar_currency AS (
	SELECT DISTINCT
		cur_1.cur_id
		,cur_1.cd
		,cur_1.name
	FROM opa_stg_uk.ar_currency cur_1
	WHERE cur_1.file_dt = (SELECT MAX(cur_2.file_dt) FROM opa_stg_uk.ar_currency cur_2 WHERE cur_1.cur_id = cur_2.cur_id)
)
	,booking_fact_margin AS (
	SELECT
		bff_1.bk_booking
		,bff_1.ffd_flag
		,bff_1.sm_currency
		,bff_1.sm_revenue
		,bff_1.sm_cnx_and_amend_revenue
		,bff_1.sm_accommodation_costs
		,bff_1.sm_early_booking_discounts
		,bff_1.sm_late_booking_discounts
		,bff_1.sm_flying_costs
		,bff_1.sm_other_costs
		,bff_1.sm_distribution_costs
		,bff_1.sm_non_margin_items
		,bff_1.sm_margin
		,bff_1.smg_currency
		,bff_1.smg_revenue
		,bff_1.smg_cnx_and_amend_revenue
		,bff_1.smg_accommodation_costs
		,bff_1.smg_early_booking_discounts
		,bff_1.smg_late_booking_discounts
		,bff_1.smg_flying_costs
		,bff_1.smg_other_costs
		,bff_1.smg_distribution_costs
		,bff_1.smg_non_margin_items
		,bff_1.smg_margin
		,bff_1.rep_currency
		,bff_1.rep_revenue
		,bff_1.rep_cnx_and_amend_revenue
		,bff_1.rep_accommodation_costs
		,bff_1.rep_early_booking_discounts
		,bff_1.rep_late_booking_discounts
		,bff_1.rep_flying_costs
		,bff_1.rep_other_costs
		,bff_1.rep_distribution_costs
		,bff_1.rep_non_margin_items
		,bff_1.rep_margin
	FROM opa_fl_uk.v_booking_fact_margin_uk bff_1
)
,booking_fact_1 AS (
	SELECT DISTINCT
		CASE WHEN bk.atcom_res_id IS NULL THEN NULL ELSE 'UKATCOM|' || bk.atcom_res_id END AS bk_booking
		,CASE WHEN
				-- All accom services cancelled
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
				=
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			THEN
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN 'U'
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
			ELSE
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN 'U'
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
		END AS bk_primary_accom
		,CASE WHEN
				-- All accom services cancelled
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
				=
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			THEN
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN 'U'
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
			ELSE
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN 'U'
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.room ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
		END AS bk_primary_room
		,COALESCE(
			CASE WHEN
				MIN(bs.min_flight_id) OVER (PARTITION BY bk.atcom_res_id) = MIN(bs.max_flight_id) OVER (PARTITION BY bk.atcom_res_id)
				AND COUNT(DISTINCT bs.min_flight_id) OVER (PARTITION BY bk.atcom_res_id) > 1
					THEN 'MULTI'
			WHEN
				MIN(bs.min_flight_id) OVER (PARTITION BY bk.atcom_res_id) = MIN(bs.max_flight_id) OVER (PARTITION BY bk.atcom_res_id)
				AND COUNT(DISTINCT bs.min_flight_id) OVER (PARTITION BY bk.atcom_res_id) = 1
					THEN 'UKATCOM|' || MIN(bs.min_flight_id) OVER (PARTITION BY bk.atcom_res_id)
			WHEN SUM(bs.flight_out_count) OVER (PARTITION BY bk.atcom_res_id) > 1
				THEN 'MULTI'
				ELSE 'UKATCOM|' || MIN(bs.min_flight_id) OVER (PARTITION BY bk.atcom_res_id)
			END
		, 'U') AS bk_first_flight
		,COALESCE(
			CASE WHEN
				MIN(bs.min_flight_id) OVER (PARTITION BY bk.atcom_res_id) = MIN(bs.max_flight_id) OVER (PARTITION BY bk.atcom_res_id)
					THEN NULL
			WHEN SUM(bs.flight_ret_count) OVER (PARTITION BY bk.atcom_res_id) > 1
				THEN 'MULTI'
				ELSE 'UKATCOM|' || MIN(bs.max_flight_id) OVER (PARTITION BY bk.atcom_res_id)
			END
		, 'U') AS bk_last_flight
		,CASE WHEN ofn.cd IS NULL
			THEN 'UKATCOM|U'
			ELSE 'UKATCOM|' || ofn.cd
		END AS bk_source_market
		,'UKATCOM' AS bk_originating_system
		,CASE WHEN
				-- Third party flight
				SUM(CASE WHEN bs.tpf_indicator = 'Y' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id) > 0
					THEN 'UKATCOM|3PF'

			WHEN
				-- Accommodation and Flight Package
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 1
				AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|PKG' -- Granular code = 'PKGAF'

			WHEN
				-- Cruise and Flight Package
				SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 1
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) > 0
				AND SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|CRU' -- Granular code = 'PKGCF'

			WHEN
				-- Cruise and Accommodation and Flight Package
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 1
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) > 0
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 1
					THEN 'UKATCOM|CRU' -- Granular code = 'PKGCAF'

			WHEN
				-- Multi Accommodation and Flight Package
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|PKG' -- Granular code = 'PKGMAF'

			WHEN
				-- Multi Cruise and Flight Package
				SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|CRU' -- Granular code = 'PKGMCF'

			WHEN
				-- Multi Accommodation and One Cruise and Flight Package
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 1
				AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
					THEN 'UKATCOM|CRU' -- Granular code = 'PKGMACF'

			WHEN
				-- Multi Cruise and One Accommodation and Flight Package
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 1
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
					THEN 'UKATCOM|CRU' -- Granular code = 'PKGMCAF'

			WHEN
				-- Multi Accommodation and Multi Cruise and Flight Package
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
					THEN 'UKATCOM|CRU' -- Granular code = 'PKGMAMCF'

			WHEN
				-- Single Accommodation Only
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 1
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|ACC' -- Granular code = 'ACCA'

			WHEN
				-- Multi Accommodation Only
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
				AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|ACC' -- Granular code = 'ACCMA'

			WHEN
				-- Accommodation and Flight Other
				SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) = 1
					THEN 'UKATCOM|PKG' -- Granular code = 'ACCOTH'

			WHEN
				-- Flight Only Return Outbound First
				SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND MIN(bs.flight_out_first_date) OVER (PARTITION BY bs.sk_booking_id) < MIN(bs.flight_ret_first_date) OVER (PARTITION BY bs.sk_booking_id)
				AND SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|FLT' -- Granular code = 'FLTROF'

			WHEN
				-- Flight Only Return Inbound First
				SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND MIN(bs.flight_ret_first_date) OVER (PARTITION BY bs.sk_booking_id) < MIN(bs.flight_out_first_date) OVER (PARTITION BY bs.sk_booking_id)
				AND SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|FLT' -- Granular code = 'FLTRIF'

			WHEN
				-- Flight only same day return
				SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND MIN(bs.flight_ret_first_date) OVER (PARTITION BY bs.sk_booking_id) = MIN(bs.flight_out_first_date) OVER (PARTITION BY bs.sk_booking_id)
				AND SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|FLT' -- Granular code = 'FLTSDR'

			WHEN
				-- Flight Only One Way Outbound
				SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) = 0
				AND (SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
					THEN 'UKATCOM|FLT' -- Granular code = 'FLTOBO'

			WHEN
				-- Flight Only One Way Inbound
				SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) = 0
				AND (SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
					THEN 'UKATCOM|FLT' -- Granular code = 'FLTIBO'

			WHEN
				-- Single Cruise Only
				SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 1
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
				AND SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 0
					THEN 'UKATCOM|CRU' -- Granular code = 'CRUSGL'

			WHEN
				-- Multi Cruise Only
				SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) > 1
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
					THEN 'UKATCOM|CRU' -- Granular code = 'CRUMLT'

			WHEN
				-- Third party
				SUM(bs.thirdparty_count) OVER (PARTITION BY bs.sk_booking_id) > 0
				AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
				AND (SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id)) = 0
					THEN 'UKATCOM|TPB' -- Granular code = 'TPB'

			ELSE 'UKATCOM|OTH'
		END AS bk_booking_type
		,CASE WHEN bk.booking_status IS NULL
			THEN 'U'
			ELSE 'UKATCOM|' || bk.booking_status
		END AS bk_booking_status
		,bk.atcom_res_id AS source_booking_id
		,bk.atcom_res_version AS source_booking_version
		,COALESCE(CAST(bk.source_created_on AS TIMESTAMP), CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS booking_created_datetime
		,COALESCE(bk.confirmed_on, CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS booking_confirmed_datetime
		,COALESCE(bk.cancelled_on, CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS booking_cancelled_datetime
		,gs.group_season_code AS group_season
		,CASE WHEN bk.sk_season_id IS NULL OR bk.sk_season_id = -1 OR bk.sk_season_id = -2
			THEN NULL
			ELSE
				CASE WHEN SUBSTRING(bk.sk_season_id, 5, 2) = 01
					THEN 'S' || SUBSTRING(bk.sk_season_id, 3, 2)
					ELSE 'W' || SUBSTRING(bk.sk_season_id, 3, 2)
				END
		END AS sm_season
		,CASE WHEN uc.cd IS NULL
			THEN NULL
			ELSE 'UKATCOM|' || uc.cd
		END AS channel_code
		,CASE WHEN uc.name IS NULL
			THEN NULL
			ELSE 'UKATCOM|' || uc.name
		END AS channel_desc
		,CASE WHEN
				-- All accom services cancelled
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
				=
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			THEN
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN NULL
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
			ELSE
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN NULL
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
		END AS booked_board_code
		,CASE WHEN
				-- All accom services cancelled
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
				=
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
					+ SUM(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			THEN
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN NULL
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.board_name ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' THEN bs.board_name ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
			ELSE
				CASE WHEN
						COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
						= 0
					THEN NULL
					ELSE
						CASE WHEN
							COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							+ COUNT(DISTINCT CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.board_cd ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							> 1
						THEN 'MULTI'

						ELSE 'UKATCOM|' || COALESCE(
								MIN(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.board_name ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
								,MIN(CASE WHEN bs.service_type = 'AHOC' AND bs.sell_type = 'ACCM' AND bs.service_status = 'CON' THEN bs.board_name ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id)
							)
						END
				END
		END AS booked_board_name
		,CASE WHEN
				-- All accom services are cancelled
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
				= SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			THEN
				CASE WHEN SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id) > 1
					THEN 'Y'
					ELSE 'N'
				END
			ELSE
				CASE WHEN SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id) > 1
					THEN 'Y'
					ELSE 'N'
				END
		END AS multi_room_booking
		,CASE WHEN
				-- All accom services are cancelled
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
				= SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			THEN
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			ELSE
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
		END AS number_of_booked_rooms
		,CASE WHEN
				-- All accom services are cancelled
				SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
				= SUM(CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CNX' THEN 1 ELSE 0 END) OVER (PARTITION BY bs.sk_booking_id)
			THEN
				CASE WHEN COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id) > 1
					THEN 'Y'
					ELSE 'N'
				END
			ELSE
				CASE WHEN COUNT(DISTINCT CASE WHEN bs.service_type = 'ACC' AND bs.source_stock_type_code NOT IN ('MC') AND bs.service_status = 'CON' THEN bs.accom ELSE NULL END) OVER (PARTITION BY bs.sk_booking_id) > 1
					THEN 'Y'
					ELSE 'N'
				END
		END AS multi_centre_booking
		,COALESCE(bk.season_date,CAST('2999-12-31 23:59:59.0' AS DATE)) AS departure_date
		,COALESCE(
			bs.max_multi_center_date,
			bs.max_accom_date,
			bs.max_cruise_date,
			bs.max_flight_date,
			CAST('2999-12-31 23:59:59.0' AS DATE)
		) AS return_date
		,CASE WHEN
			COALESCE(bk.season_date, CAST('2999-12-31 23:59:59.0' AS DATE)) = CAST('2999-12-31 23:59:59.0' AS DATE)
		OR
			COALESCE(
				bs.max_multi_center_date,
				bs.max_accom_date,
				bs.max_cruise_date,
				bs.max_flight_date,
				CAST('2999-12-31 23:59:59.0' AS DATE)
			) = CAST('2999-12-31 23:59:59.0' AS DATE)
		THEN 0
		ELSE
			DATEDIFF('DAY',
				bk.season_date
			,
				COALESCE(
					bs.max_multi_center_date,
					bs.max_accom_date,
					bs.max_cruise_date,
					bs.max_flight_date)
			)
		END	AS DURATION
		,COALESCE(bk.number_of_adults, 0) AS std_number_of_booking_adult_pax
		,COALESCE(bk.number_of_children, 0) AS std_number_of_booking_child_pax
		,COALESCE(bk.number_of_infants, 0) AS std_number_of_booking_infant_pax
		,COALESCE(bk.number_of_passengers, 0) AS std_number_of_booking_pax
		,COALESCE(bk.number_of_adults, 0) AS sm_number_of_booking_adult_pax
		,0 AS sm_number_of_booking_teenager_pax
		,COALESCE(bk.number_of_children, 0) AS sm_number_of_booking_child_pax
		,COALESCE(bk.number_of_infants, 0) AS sm_number_of_booking_infant_pax
		,COALESCE(bk.number_of_passengers, 0) AS sm_number_of_booking_pax
		,CASE WHEN COUNT(DISTINCT bs.min_flight_gateway) OVER (PARTITION BY bk.atcom_res_id) > 1
			THEN 'MULTI'
			ELSE MIN(bs.min_flight_gateway) OVER (PARTITION BY bk.atcom_res_id)
		END AS primary_gateway
		,cur.name AS currency
		,COALESCE(bk.dwh_created_on, CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS sm_created_datetime
		,COALESCE(bk.dwh_modified_on, CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS sm_updated_datetime
		,CAST(CONVERT_TIMEZONE('Europe/London',CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ) AS dm_created_datetime

	FROM fl_acr_booking bk
	LEFT OUTER JOIN booking_service bs ON bk.sk_booking_id = bs.sk_booking_id AND bk.booking_version = bs.booking_version

	-- Group Season
	LEFT OUTER JOIN dates gs ON CAST(COALESCE(SUBSTRING(bk.season_date, 1, 4) || SUBSTRING(bk.season_date, 6, 2) || SUBSTRING(bk.season_date, 9, 2), 20991231) AS INTEGER) = gs.bk_date

	-- Market source
	LEFT OUTER JOIN ar_agent ag ON bk.atcom_agent_id = ag.agt_id

      -- V1.06 Version of source market joins
      LEFT OUTER JOIN ar_market m   ON bk.atcom_market_id = m.mkt_id
      LEFT OUTER JOIN ar_officename ofn ON m.off_id = ofn.off_name_id

	-- Channel
	LEFT OUTER JOIN ar_usercodes uc ON ag.agt_tp_id = uc.user_cd_id

	-- Currency
	LEFT OUTER JOIN ar_currency cur ON bk.atcom_sell_currency_id = cur.cur_id

	WHERE bk_booking IS NOT NULL
)

SELECT
	bf.bk_booking,
	bf.bk_primary_accom,
	bf.bk_primary_room,
	bf.bk_first_flight,
	bf.bk_last_flight,
	bf.bk_source_market,
	bf.bk_originating_system,
	bf.bk_booking_type,
	bf.bk_booking_status,
	bf.source_booking_id,
	bf.source_booking_version,
	bf.booking_created_datetime,
	bf.booking_confirmed_datetime,
	bf.booking_cancelled_datetime,
	bf.group_season,
	bf.sm_season,
	bf.channel_code,
	bf.channel_desc,
	bf.booked_board_code,
	bf.booked_board_name,
	bf.multi_room_booking,
	bf.number_of_booked_rooms,
	bf.multi_centre_booking,
	bf.departure_date,
	bf.return_date,
	bf.duration,
	bf.std_number_of_booking_adult_pax,
	bf.std_number_of_booking_child_pax,
	bf.std_number_of_booking_infant_pax,
	bf.std_number_of_booking_pax,
	bf.sm_number_of_booking_adult_pax,
	bf.sm_number_of_booking_teenager_pax,
	bf.sm_number_of_booking_child_pax,
	bf.sm_number_of_booking_infant_pax,
	bf.sm_number_of_booking_pax,
	bf.primary_gateway,
		COALESCE(bfm.sm_currency, fx.bk_sm_ccy, 'U') AS sm_currency,
	COALESCE(bfm.sm_revenue, 0) AS sm_revenue,
	COALESCE(bfm.sm_cnx_and_amend_revenue, 0) AS sm_cnx_and_amend_revenue,
	COALESCE(bfm.sm_accommodation_costs, 0) AS sm_accommodation_costs,
	COALESCE(bfm.sm_early_booking_discounts, 0) AS sm_early_booking_discounts,
	COALESCE(bfm.sm_late_booking_discounts, 0) AS sm_late_booking_discounts,
	COALESCE(bfm.sm_flying_costs, 0) AS sm_flying_costs,
	COALESCE(bfm.sm_other_costs, 0) AS sm_other_costs,
	COALESCE(bfm.sm_distribution_costs, 0) AS sm_distribution_costs,
	COALESCE(bfm.sm_non_margin_items, 0) AS sm_non_margin_items,
	COALESCE(bfm.sm_margin, 0) AS sm_margin,
	COALESCE(bfm.smg_currency, fx.bk_smg_ccy, 'U') AS smg_currency,
	COALESCE(bfm.smg_revenue, 0) AS smg_revenue,
	COALESCE(bfm.smg_cnx_and_amend_revenue, 0) AS smg_cnx_and_amend_revenue,
	COALESCE(bfm.smg_accommodation_costs, 0) AS smg_accommodation_costs,
	COALESCE(bfm.smg_early_booking_discounts, 0) AS smg_early_booking_discounts,
	COALESCE(bfm.smg_late_booking_discounts, 0) AS smg_late_booking_discounts,
	COALESCE(bfm.smg_flying_costs, 0) AS smg_flying_costs,
	COALESCE(bfm.smg_other_costs, 0) AS smg_other_costs,
	COALESCE(bfm.smg_distribution_costs, 0) AS smg_distribution_costs,
	COALESCE(bfm.smg_non_margin_items, 0) AS smg_non_margin_items,
	COALESCE(bfm.smg_margin, 0) AS smg_margin,
	COALESCE(bfm.rep_currency, fx.bk_rep_ccy, 'U') AS rep_currency,
	COALESCE(bfm.rep_revenue, 0) AS rep_revenue,
	COALESCE(bfm.rep_cnx_and_amend_revenue, 0) AS rep_cnx_and_amend_revenue,
	COALESCE(bfm.rep_accommodation_costs, 0) AS rep_accommodation_costs,
	COALESCE(bfm.rep_early_booking_discounts, 0) AS rep_early_booking_discounts,
	COALESCE(bfm.rep_late_booking_discounts, 0) AS rep_late_booking_discounts,
	COALESCE(bfm.rep_flying_costs, 0) AS rep_flying_costs,
	COALESCE(bfm.rep_other_costs, 0) AS rep_other_costs,
	COALESCE(bfm.rep_distribution_costs, 0) AS rep_distribution_costs,
	COALESCE(bfm.rep_non_margin_items, 0) AS rep_non_margin_items,
	COALESCE(bfm.rep_margin, 0) AS rep_margin,
	COALESCE(bfm.ffd_flag, 'N') AS ffd_flag,
	bf.sm_created_datetime,
	bf.sm_updated_datetime,
	bf.dm_created_datetime
FROM booking_fact_1 bf
LEFT OUTER JOIN booking_fact_margin bfm ON bf.bk_booking = bfm.bk_booking
LEFT OUTER JOIN opa_fl_all.source_market sm ON bf.bk_source_market = sm.bk_source_market
LEFT OUTER JOIN opa_fl_uk.fx_rates_dim_uk fx
	ON bf.sm_season = fx.bk_season
	AND sm.source_market_code = fx.source_market_code
