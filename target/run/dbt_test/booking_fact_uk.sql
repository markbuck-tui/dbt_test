create or replace transient table OPA_DEV.DBT_TEST.booking_fact_uk
      as (

WITH booking_service AS (
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
	FROM OPA_DEV.DBT_TEST.with_fl_acr_booking_service bk_ser
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_fl_acr_service ser ON bk_ser.sk_service_id = ser.sk_service_id AND bk_ser.service_version = ser.service_version
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_fl_acr_service_element ser_e ON ser.sk_service_id = ser_e.sk_service_id AND ser.service_version = ser_e.service_version

	-- Accom
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_sellstatic sls ON ser.atcom_ser_id = sls.sell_stc_id
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_staticstock sts ON sls.stc_stk_id = sts.stc_stk_id

	-- Room
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_sellunit su ON ser_e.atcom_sub_ser_id = su.sell_unit_id
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_staticroom str ON sts.stc_stk_id = str.stc_stk_id AND su.rm_id = str.rm_id

	-- Board
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_usercodes uc_3 ON su.bb_cd_id = uc_3.user_cd_id

	-- Flight
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_transinvroute tir ON ser.atcom_ser_id = tir.trans_inv_route_id
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_transroute tr ON tir.trans_route_id = tr.trans_route_id
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_transinvroutesector tirs ON tir.trans_inv_route_id = tirs.trans_inv_route_id
  LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_transinvsector tis ON tirs.trans_inv_sec_id = tis.trans_inv_sec_id
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_point dpt ON ser.atcom_dep_point_id = dpt.pt_id
	LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_point apt ON ser.atcom_arr_point_id = apt.pt_id

	ORDER BY
		bk_ser.sk_booking_id
		,bk_ser.booking_version
		,ser.source_stock_type_code

)
,booking_fact_1 AS (
  SELECT DISTINCT
    CASE WHEN bk.atcom_res_id IS NULL THEN NULL ELSE 'UKATCOM|' || bk.atcom_res_id || '|' || bk.booking_version END AS bk_booking
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
          THEN 'UKATCOM|PKG' -- Granular code = 'PKGCF'

      WHEN
        -- Cruise and Accommodation and Flight Package
        SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 1
        AND (SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) + SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id)) > 0
        AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 1
          THEN 'UKATCOM|PKG' -- Granular code = 'PKGCAF'

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
          THEN 'UKATCOM|PKG' -- Granular code = 'PKGMCF'

      WHEN
        -- Multi Accommodation and One Cruise and Flight Package
        SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) > 1
        AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) = 1
        AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
        AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
          THEN 'UKATCOM|PKG' -- Granular code = 'PKGMACF'

      WHEN
        -- Multi Cruise and One Accommodation and Flight Package
        SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) = 1
        AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) > 1
        AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
        AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
          THEN 'UKATCOM|PKG' -- Granular code = 'PKGMCAF'

      WHEN
        -- Multi Accommodation and Multi Cruise and Flight Package
        SUM(bs.accom_count) OVER (PARTITION BY bs.sk_booking_id) > 1
        AND SUM(bs.cruise_count) OVER (PARTITION BY bs.sk_booking_id) > 1
        AND SUM(bs.flight_out_count) OVER (PARTITION BY bs.sk_booking_id) > 0
        AND SUM(bs.flight_ret_count) OVER (PARTITION BY bs.sk_booking_id) > 0
          THEN 'UKATCOM|PKG' -- Granular code = 'PKGMAMCF'

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
          THEN 'UKATCOM|ACC' -- Granular code = 'ACCOTH'

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
    --,bk.atcom_res_version AS source_booking_version
    ,bk.booking_version AS source_booking_version -- swapped to FL version not Atcom version PiT
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
    -- ,cur.name AS currency
    ,'Insert' AS record_type
    ,COALESCE(bk.effective_from, CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS effective_from
    ,COALESCE(
      bk.lead_effective_from
      ,bk.effective_to
      ,CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)
    ) AS effective_to
    ,COALESCE(bk.dwh_created_on, CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS sm_created_datetime
    ,COALESCE(bk.dwh_modified_on, CAST('2999-12-31 23:59:59.0' AS TIMESTAMP)) AS sm_updated_datetime
    ,CAST(CONVERT_TIMEZONE('Europe/London',CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ) AS dm_created_datetime

  FROM OPA_DEV.DBT_TEST.with_fl_acr_booking bk
  LEFT OUTER JOIN booking_service bs ON bk.sk_booking_id = bs.sk_booking_id AND bk.booking_version = bs.booking_version

  -- Group Season
  LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_dates gs ON CAST(COALESCE(SUBSTRING(bk.season_date, 1, 4) || SUBSTRING(bk.season_date, 6, 2) || SUBSTRING(bk.season_date, 9, 2), 20991231) AS INTEGER) = gs.bk_date

  -- Market source
  LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_agent ag ON bk.atcom_agent_id = ag.agt_id

  -- V1.06 Version of source market joins
  LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_market m ON bk.atcom_market_id = m.mkt_id
  LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_officename ofn ON m.off_id = ofn.off_name_id

  -- Channel
  LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_ar_usercodes uc ON ag.agt_tp_id = uc.user_cd_id

  -- Currency
  -- LEFT OUTER JOIN ar_currency cur ON bk.atcom_sell_currency_id = cur.cur_id

  WHERE bk_booking IS NOT NULL
)

SELECT
  bf_1.bk_booking,
  bf_1.bk_primary_accom,
  bf_1.bk_primary_room,
  bf_1.bk_first_flight,
  bf_1.bk_last_flight,
  bf_1.bk_source_market,
  bf_1.bk_originating_system,
  bf_1.bk_booking_type,
  bf_1.bk_booking_status,
  bf_1.source_booking_id,
  bf_1.source_booking_version,
  bf_1.booking_created_datetime,
  bf_1.booking_confirmed_datetime,
  bf_1.booking_cancelled_datetime,
  bf_1.group_season,
  bf_1.sm_season,
  bf_1.channel_code,
  bf_1.channel_desc,
  bf_1.booked_board_code,
  bf_1.booked_board_name,
  bf_1.multi_room_booking,
  bf_1.number_of_booked_rooms,
  bf_1.multi_centre_booking,
  bf_1.departure_date,
  bf_1.return_date,
  bf_1.duration,
  bf_1.std_number_of_booking_adult_pax,
  bf_1.std_number_of_booking_child_pax,
  bf_1.std_number_of_booking_infant_pax,
  bf_1.std_number_of_booking_pax,
  bf_1.sm_number_of_booking_adult_pax,
  bf_1.sm_number_of_booking_teenager_pax,
  bf_1.sm_number_of_booking_child_pax,
  bf_1.sm_number_of_booking_infant_pax,
  bf_1.sm_number_of_booking_pax,
  bf_1.primary_gateway,
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
  bf_1.record_type,
  bf_1.effective_from,
  bf_1.effective_to,
  bf_1.sm_created_datetime,
  bf_1.sm_updated_datetime,
  bf_1.dm_created_datetime,
  CASE WHEN bf_1.source_booking_version = MAX(bf_1.source_booking_version) OVER (PARTITION BY LEFT(bf_1.bk_booking, LENGTH(bf_1.bk_booking) - REGEXP_INSTR(bf_1.bk_booking, '|', 2)))
    THEN 'Y'
    ELSE 'N'
  END AS latest_record_indicator
FROM booking_fact_1 bf_1
LEFT OUTER JOIN OPA_DEV.DBT_TEST.with_booking_fact_margin bfm ON bf_1.bk_booking = bfm.bk_booking
LEFT OUTER JOIN opa_fl_all.source_market sm ON bf_1.bk_source_market = sm.bk_source_market
LEFT OUTER JOIN opa_fl_uk.fx_rates_dim_uk fx
  ON bf_1.sm_season = fx.bk_season
  AND sm.source_market_code = fx.source_market_code
ORDER BY
  bk_booking
  ,source_booking_version
  ,record_type DESC
      );
    