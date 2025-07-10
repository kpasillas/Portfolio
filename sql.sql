SELECT
    c.account_number AS customer_code,
    c.customer_name AS customer_name,
    poj.name AS pickup_name,
    poj.address_one AS pickup_address_one,
    poj.address_two AS pickup_address_two,
    poj.city AS pickup_city,
    poj.state AS pickup_state,
    poj.zip AS pickup_zip,
    doj.driver_id AS driver_id,
    doj.driver_name AS driver_name,
    o.order_number AS order_number,
    o.reference_one AS reference_one,
    o.reference_two AS reference_two,
    doj.name AS delivery_name,
    doj.address_one AS delivery_address_one,
    doj.address_two AS delivery_address_two,
    doj.city AS delivery_city,
    doj.state AS delivery_state,
    doj.zip AS delivery_zip,
    doj.phone AS delivery_phone,
    op.weight AS weight,
    s.name AS service_type,
    COALESCE(t.name, "Missing Terminal") AS terminal,
	COALESCE(z.name, "Missing Zone") AS zone,
    o.notes AS notes,
	CAST(convert_from_utc_to_local_time(o.due_date, doj.zip, doj.state) AS DATE) AS 'due_date',
	CAST(convert_from_utc_to_local_time(o.order_date, doj.zip, doj.state) AS DATE) AS 'order_date',
	convert_from_utc_to_local_time(sort_event.event_timestamp, doj.zip, doj.state) AS 'sortscan_time',
    sort_event.event_note AS 'sortscan_note',
	convert_from_utc_to_local_time(arrive_hub_event.event_timestamp, doj.zip, doj.state) AS 'hub_arrival_time',
    arrive_hub_event.event_note AS 'hub_arrival_note',
	convert_from_utc_to_local_time(out_for_delivery_event.event_timestamp, doj.zip, doj.state) AS 'out_for_delivery_time',
    out_for_delivery_event.event_note AS 'out_for_delivery_note',
	convert_from_utc_to_local_time(attempted_event.event_timestamp, doj.zip, doj.state) AS 'attempted_time',
    attempted_event.event_note AS 'attempted_note',
	convert_from_utc_to_local_time(delayed_event.event_timestamp, doj.zip, doj.state) AS 'delayed_time',
    delayed_event.event_note AS 'delayed_note',
	convert_from_utc_to_local_time(delivered_event.event_timestamp, doj.zip, doj.state) AS 'delivered_time',
    delivered_event.event_note AS 'delivered_note',
	convert_from_utc_to_local_time(deleted_event.event_timestamp, doj.zip, doj.state) AS 'deleted_time',
    deleted_event.event_note AS 'deleted_note',
	convert_from_utc_to_local_time(rts_event.event_timestamp, doj.zip, doj.state) AS 'rts_time',
    rts_event.event_note AS 'rts_note',
    o.pod_name AS pod_name,
    ofs.amount AS service_amount,
    ofr.amount AS revenue,
    off.amount AS fuel_fee,
    (CASE
		WHEN (c.account_number IN (
			'10000', '10049', '10051', '10053', '10055', '79000', '100100', '100463', '100935', '101999', '102292', '102378', '102425', '102454', '102458', '102487', '102491', '102494', '102495', '102629', '102630', '102676', '102677', '102708', '102709', '102761', '102762', '102830', '102836', '103009', '103010', '103026', '103326', '103434', '103463', '103491', '103522', '989898998', '989898999'
		)) THEN -1		-- pick-up/internal accounts		
		WHEN (c.account_number IN (
			'99004', '44145', '99011', '44144', '99442', '99001', '99280', '44132', '99173', '99008', '99000', '99445', '99608', '99015', '99017', '99007', '92272', '99583', '99013', '99010', '99005', '99006', '99635', '99014', '99002', '99016', '99009', '99018', '99174'
		)) THEN -2		-- ACE customers
		WHEN (c.account_number IN ('102705')) THEN -2		-- Mistake
		WHEN (c.account_number IN ('103701', '103699')) THEN 0		-- transit/linehaul revenue
		WHEN (c.account_number IN ('102664', '103645', '103262')) THEN 0		-- Monthly billing account
		WHEN ((COALESCE(ofr.amount, 0) < 1) AND (c.account_number NOT IN ('102214', '102215', '102252', '102253', '102289', '102392', '102392W2', '102394', '102449', '102499', '102501', '102501W2', '102511', '102515', '102738', '103349', '6466', 'PA0080', 'PA0087', 'PA0580', 'PA0580'))) THEN 0
		ELSE 1
	END) AS 'package_operational',
    (CASE
		WHEN (c.account_number IN (
			'10000', '10049', '10051', '10053', '10055', '79000', '100100', '100463', '100935', '101999', '102292', '102378', '102425', '102454', '102458', '102487', '102491', '102494', '102495', '102629', '102630', '102676', '102677', '102708', '102709', '102761', '102762', '102830', '102836', '103009', '103010', '103026', '103326', '103434', '103463', '103491', '103522', '989898998', '989898999'
		)) THEN -1		-- pick-up/internal accounts		
		WHEN (c.account_number IN (
			'99004', '44145', '99011', '44144', '99442', '99001', '99280', '44132', '99173', '99008', '99000', '99445', '99608', '99015', '99017', '99007', '92272', '99583', '99013', '99010', '99005', '99006', '99635', '99014', '99002', '99016', '99009', '99018', '99174'
		)) THEN -2		-- ACE customers
		WHEN (c.account_number IN ('102705')) THEN -2		-- Mistake
		WHEN (c.account_number IN ('103701', '103699')) THEN 0		-- transit/linehaul revenue
		WHEN (c.account_number IN ('102664', '103645', '103262')) THEN 0		-- Monthly billing account
		WHEN (COALESCE(ofr.amount, 0) < 1) THEN 0
		ELSE 1
	END) AS 'package_financial',
    (CASE
        WHEN (delivered_event.event_timestamp IS NOT NULL) THEN "Delivered"
		WHEN (s.name = "RTS") THEN "Returned to Sender"
        WHEN (GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01')) = '1900-01-01') THEN "Created"
        WHEN (deleted_event.event_timestamp = GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01'))) THEN "Deleted"
        WHEN (delivered_event.event_timestamp = GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01'))) THEN "Delivered"
        WHEN (delayed_event.event_timestamp = GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01'))) THEN "Delayed"
        WHEN (attempted_event.event_timestamp = GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01'))) THEN "Attempted"
        WHEN (out_for_delivery_event.event_timestamp = GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01'))) THEN "Out for Delivery"
        WHEN ((sort_event.event_timestamp = GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01'))) OR ((arrive_hub_event.event_timestamp = GREATEST(COALESCE(sort_event.event_timestamp, '1900-01-01'), COALESCE(arrive_hub_event.event_timestamp, '1900-01-01'), COALESCE(out_for_delivery_event.event_timestamp, '1900-01-01'), COALESCE(attempted_event.event_timestamp, '1900-01-01'), COALESCE(delayed_event.event_timestamp, '1900-01-01'), COALESCE(delivered_event.event_timestamp, '1900-01-01'), COALESCE(deleted_event.event_timestamp, '1900-01-01'))))) THEN "Received"
        ELSE "Unknown Status"
    END) AS 'status'

FROM
    integrity.orders o 
    LEFT JOIN integrity.customers c ON (c.id = o.customer_id)
    LEFT JOIN integrity.order_jobs poj ON ((o.id = poj.order_id) AND (poj.job_type = 'P'))
    LEFT JOIN integrity.order_jobs doj ON ((o.id = doj.order_id) AND (doj.job_type = 'D'))
    LEFT JOIN integrity.order_pieces op ON (o.id = op.order_id)
    LEFT JOIN integrity.services s ON (o.service_id = s.id)
	LEFT JOIN (
		SELECT *
		FROM integrity.zip_codes zc1
		WHERE
			zc1.id = (
				SELECT zc2.id
				FROM integrity.zip_codes zc2
				WHERE
					zc1.zip = zc2.zip
				ORDER BY zc2.updated_at DESC
				LIMIT 1
			)
	) zc ON (zc.zip = LEFT(doj.zip, 5))
    LEFT JOIN integrity.terminals t ON (zc.terminal_id = t.id)
	LEFT JOIN (
		SELECT *
		FROM integrity.zones z1
		WHERE
			z1.id = (
				SELECT z2.id
				FROM integrity.zones z2
				WHERE
					z1.name = z2.name
				ORDER BY z2.updated_at DESC
				LIMIT 1
			)
	) z ON (z.id = zc.zone_id)
    LEFT JOIN (
		SELECT *
		FROM integrity.order_events sort_event_1
		WHERE
				sort_event_1.event_id = 6
			AND	sort_event_1.id = (
				SELECT sort_event_2.id
				FROM integrity.order_events sort_event_2
				WHERE
						sort_event_2.event_id = 6
					AND	sort_event_1.order_id = sort_event_2.order_id
				ORDER BY sort_event_2.event_timestamp
				LIMIT 1
			)
	) sort_event ON (o.id = sort_event.order_id)
	LEFT JOIN (
		SELECT *
		FROM integrity.order_events arrive_hub_event_1
		WHERE
				arrive_hub_event_1.event_id = 8
			AND	arrive_hub_event_1.id = (
				SELECT arrive_hub_event_2.id
				FROM integrity.order_events arrive_hub_event_2
				WHERE
						arrive_hub_event_2.event_id = 8
					AND	arrive_hub_event_1.order_id = arrive_hub_event_2.order_id
				ORDER BY arrive_hub_event_2.event_timestamp
				LIMIT 1
			)
	) arrive_hub_event ON (o.id = arrive_hub_event.order_id)
	LEFT JOIN (
		SELECT *
		FROM integrity.order_events out_for_delivery_event_1
		WHERE
				out_for_delivery_event_1.event_id = 10
			AND	out_for_delivery_event_1.event_note <> ''
			AND	out_for_delivery_event_1.id = (
				SELECT out_for_delivery_event_2.id
				FROM integrity.order_events out_for_delivery_event_2
				WHERE
						out_for_delivery_event_2.event_id = 10
					AND	out_for_delivery_event_2.event_note <> ''
					AND	out_for_delivery_event_1.order_id = out_for_delivery_event_2.order_id
				ORDER BY out_for_delivery_event_2.event_timestamp
				LIMIT 1
			)
	) out_for_delivery_event ON (o.id = out_for_delivery_event.order_id)
	LEFT JOIN (
		SELECT *
		FROM integrity.order_events attempted_event_1
		WHERE
				attempted_event_1.event_id = 12
			AND	attempted_event_1.id = (
				SELECT attempted_event_2.id
				FROM integrity.order_events attempted_event_2
				WHERE
						attempted_event_2.event_id = 12
					AND	attempted_event_1.order_id = attempted_event_2.order_id
				ORDER BY attempted_event_2.event_timestamp
				LIMIT 1
			)
	) attempted_event ON (o.id = attempted_event.order_id)
    LEFT JOIN (
		SELECT *
		FROM integrity.order_events delayed_event_1
		WHERE
				delayed_event_1.event_id = 13
			AND	delayed_event_1.id = (
				SELECT delayed_event_2.id
				FROM integrity.order_events delayed_event_2
				WHERE
						delayed_event_2.event_id = 13
					AND	delayed_event_1.order_id = delayed_event_2.order_id
				ORDER BY delayed_event_2.event_timestamp
				LIMIT 1
			)
	) delayed_event ON (o.id = delayed_event.order_id)
	LEFT JOIN (
		SELECT *
		FROM integrity.order_events delivered_event_1
		WHERE
				delivered_event_1.event_id = 11
			AND	delivered_event_1.id = (
				SELECT delivered_event_2.id
				FROM integrity.order_events delivered_event_2
				WHERE
						delivered_event_2.event_id = 11
					AND	delivered_event_1.order_id = delivered_event_2.order_id
				ORDER BY delivered_event_2.event_timestamp
				LIMIT 1
			)
	) delivered_event ON (o.id = delivered_event.order_id)
	LEFT JOIN (
		SELECT *
		FROM integrity.order_events deleted_event_1
		WHERE
				deleted_event_1.event_id = 2
			AND	deleted_event_1.id = (
				SELECT deleted_event_2.id
				FROM integrity.order_events deleted_event_2
				WHERE
						deleted_event_2.event_id = 2
					AND	deleted_event_1.order_id = deleted_event_2.order_id
				ORDER BY deleted_event_2.event_timestamp
				LIMIT 1
			)
	) deleted_event ON (o.id = deleted_event.order_id)
	LEFT JOIN (
		SELECT *
		FROM integrity.order_events rts_event_1
		WHERE
				rts_event_1.event_id = 14
			AND	rts_event_1.id = (
				SELECT rts_event_2.id
				FROM integrity.order_events rts_event_2
				WHERE
						rts_event_2.event_id = 14
					AND	rts_event_1.order_id = rts_event_2.order_id
				ORDER BY rts_event_2.event_timestamp
				LIMIT 1
			)
	) rts_event ON (o.id = rts_event.order_id)
	LEFT JOIN (
		SELECT *
		FROM integrity.order_fees ofr1
		WHERE
				ofr1.name = 'AmountCharged'
			AND ofr1.id = (
					SELECT ofr2.id
					FROM integrity.order_fees ofr2
					WHERE
							ofr2.order_id = ofr1.order_id
						AND ofr2.name = 'AmountCharged'
					ORDER BY ofr2.created_at DESC
					LIMIT 1
			)
	) ofr ON (o.id = ofr.order_id)
    LEFT JOIN (
		SELECT *
		FROM integrity.order_fees ofs1
		WHERE
				ofs1.name = 'Service'
			AND ofs1.id = (
					SELECT ofs2.id
					FROM integrity.order_fees ofs2
					WHERE
							ofs2.order_id = ofs1.order_id
						AND ofs2.name = 'Service'
					ORDER BY ofs2.created_at DESC
					LIMIT 1
			)
	) ofs ON (o.id = ofs.order_id)
    LEFT JOIN (
		SELECT *
		FROM integrity.order_fees off1
		WHERE
				off1.name = 'Fuel'
			AND off1.id = (
					SELECT off2.id
					FROM integrity.order_fees off2
					WHERE
							off2.order_id = off1.order_id
						AND off2.name = 'Fuel'
					ORDER BY off2.created_at DESC
					LIMIT 1
			)
	) off ON (o.id = off.order_id)
    LEFT JOIN (
		SELECT *
		FROM integrity.order_fees ofra1
		WHERE
				ofra1.name = 'Reattempt'
			AND ofra1.id = (
					SELECT ofra2.id
					FROM integrity.order_fees ofra2
					WHERE
							ofra2.order_id = ofra1.order_id
						AND ofra2.name = 'Reattempt'
					ORDER BY ofra2.created_at DESC
					LIMIT 1
			)
	) ofra ON (o.id = ofra.order_id)