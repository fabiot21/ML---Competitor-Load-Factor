def getCompetitorQuery(competitor, country):
	query = '''
	SELECT
	  GENERATE_UUID() AS uuid,
	  route,
	  departure_date,
	  departure_hour,
	  departure_time,
	  flight_no,
	  ARRAY_AGG(STRUCT(ap, price)) data
	FROM
	(
	  SELECT
	    observation_date,
	    CONCAT(origin, destination) as route,
	    flight_no,
	    departure_date,
	    departure_time,
	    DATE_DIFF(departure_date, observation_date, DAY) as ap,
	    MAX(price) AS price,
	    (
	        CASE
	          WHEN PARSE_TIME("%R", departure_time) < TIME(2, 0, 0) THEN 1
	          WHEN PARSE_TIME("%R", departure_time) < TIME(3, 0, 0) THEN 2
	          WHEN PARSE_TIME("%R", departure_time) < TIME(4, 0, 0) THEN 3
	          WHEN PARSE_TIME("%R", departure_time) < TIME(5, 0, 0) THEN 4
	          WHEN PARSE_TIME("%R", departure_time) < TIME(6, 0, 0) THEN 5
	          WHEN PARSE_TIME("%R", departure_time) < TIME(7, 0, 0) THEN 6
	          WHEN PARSE_TIME("%R", departure_time) < TIME(8, 0, 0) THEN 7
	          WHEN PARSE_TIME("%R", departure_time) < TIME(9, 0, 0) THEN 8
	          WHEN PARSE_TIME("%R", departure_time) < TIME(10, 0, 0) THEN 9
	          WHEN PARSE_TIME("%R", departure_time) < TIME(11, 0, 0) THEN 10
	          WHEN PARSE_TIME("%R", departure_time) < TIME(12, 0, 0) THEN 11
	          WHEN PARSE_TIME("%R", departure_time) < TIME(13, 0, 0) THEN 12
	          WHEN PARSE_TIME("%R", departure_time) < TIME(14, 0, 0) THEN 13
	          WHEN PARSE_TIME("%R", departure_time) < TIME(15, 0, 0) THEN 14
	          WHEN PARSE_TIME("%R", departure_time) < TIME(16, 0, 0) THEN 15
	          WHEN PARSE_TIME("%R", departure_time) < TIME(17, 0, 0) THEN 16
	          WHEN PARSE_TIME("%R", departure_time) < TIME(18, 0, 0) THEN 17
	          WHEN PARSE_TIME("%R", departure_time) < TIME(19, 0, 0) THEN 18
	          WHEN PARSE_TIME("%R", departure_time) < TIME(20, 0, 0) THEN 19
	          WHEN PARSE_TIME("%R", departure_time) < TIME(21, 0, 0) THEN 20
	          WHEN PARSE_TIME("%R", departure_time) < TIME(22, 0, 0) THEN 21
	          WHEN PARSE_TIME("%R", departure_time) < TIME(23, 0, 0) THEN 22
	          WHEN PARSE_TIME("%R", departure_time) < TIME(23, 59, 59) THEN 23
	          ELSE null
	        END
	      ) as departure_hour
	  FROM
	  (

	    -- oneway data

	    SELECT
	      observation_date,
	      origin,
	      destination,
	      outbound_flight_no AS flight_no,
	      outbound_departure_date AS departure_date,
	      outbound_departure_time AS departure_time,
	      price_exc AS price
	    FROM
	      `desa-cli-aa360.IORM_INFARE_RM.INFARE_{1}`
	    WHERE
	      source LIKE '%{0}%'
	    AND
	      outbound_travel_stop_over is null
	    AND
	      inbound_travel_stop_over is null
	    AND
	      price_exc IS NOT NULL
	    AND
	      is_one_way = 1

	    UNION ALL

	    -- outbound data

	    SELECT
	      observation_date,
	      origin,
	      destination,
	      outbound_flight_no AS flight_no,
	      outbound_departure_date AS departure_date,
	      outbound_departure_time AS departure_time,
	      price_outbound AS price
	    FROM
	      `desa-cli-aa360.IORM_INFARE_RM.INFARE_{1}`
	    WHERE
	      source LIKE '%{0}%'
	    AND
	      outbound_travel_stop_over is null
	    AND
	      inbound_travel_stop_over is null
	    AND
	      price_inbound IS NOT NULL
	    AND
	      is_one_way = 0

	    UNION ALL

	    -- inbound data

	    SELECT
	      observation_date,
	      destination AS origin,
	      origin AS destination,
	      inbound_flight_no AS flight_no,
	      inbound_departure_date AS departure_date,
	      inbound_departure_time AS departure_time,
	      price_inbound AS price
	    FROM
	      `desa-cli-aa360.IORM_INFARE_RM.INFARE_{1}`
	    WHERE
	      source LIKE '%{0}%'
	    AND
	      outbound_travel_stop_over is  null
	    AND
	      inbound_travel_stop_over is null
	    AND
	      price_outbound IS NOT NULL
	    AND
	      is_one_way = 0
	  )

	  WHERE
	    observation_date >= DATE_SUB(CURRENT_DATE('-04:00'), INTERVAL 20 DAY)
	    AND departure_date <= DATE_ADD(CURRENT_DATE('-04:00'), INTERVAL 15 DAY)
	    AND departure_date >= CURRENT_DATE('-04:00')


	  GROUP BY
	    observation_date,
	    origin,
	    destination,
	    departure_date,
	    departure_time,
	    flight_no,
	    ap
	  ORDER BY
	    flight_no,
	    departure_date,
	    departure_time,
	    ap
	)

	GROUP BY
	  route,
	  departure_date,
	  departure_time,
	  departure_hour,
	  flight_no

	ORDER BY
	  route,
	  departure_date,
	  departure_time
  '''.format(competitor, country)

	return query