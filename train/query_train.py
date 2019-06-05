def getTrainQuery(DATE1, DATE2, COUNTRY, MAX_AP, STEPS):
  query = '''
  WITH airports AS (SELECT airport_code FROM `desa-cli-aa360.IORM_P_TABLES.p_geographic_model` WHERE country_code = '{2}')
  SELECT
    ruta as route,
    fec_vuelo as departure_date,
    franja as departure_hour,
    num_vuelo as flight_no,
    ARRAY_AGG(STRUCT(AP AS ap, price, FO AS lf)) data
  FROM
  (
  SELECT
    table_fo.*,
    table_infare.price
  FROM
  (
    SELECT 
      CONCAT(Orgn, Dstn) as ruta,
      PARSE_DATE("%F", scheffdate) as fec_vista,
      (
        CASE
          WHEN Cap > 0 THEN Tot_Bkd/Cap
          ELSE 0
        END
      ) as FO,
      PARSE_DATE("%F", Dept_Date) as fec_vuelo,
      PARSE_TIME("%R", Dept_Time) as outbound_departure_time,
      (
        CASE
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(2, 0, 0) THEN 1
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(3, 0, 0) THEN 2
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(4, 0, 0) THEN 3
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(5, 0, 0) THEN 4
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(6, 0, 0) THEN 5
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(7, 0, 0) THEN 6
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(8, 0, 0) THEN 7
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(9, 0, 0) THEN 8
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(10, 0, 0) THEN 9
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(11, 0, 0) THEN 10
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(12, 0, 0) THEN 11
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(13, 0, 0) THEN 12
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(14, 0, 0) THEN 13
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(15, 0, 0) THEN 14
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(16, 0, 0) THEN 15
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(17, 0, 0) THEN 16
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(18, 0, 0) THEN 17
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(19, 0, 0) THEN 18
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(20, 0, 0) THEN 19
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(21, 0, 0) THEN 20
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(22, 0, 0) THEN 21
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(23, 0, 0) THEN 22
          WHEN PARSE_TIME("%R", Dept_Time) < TIME(23, 59, 59) THEN 23
          ELSE null
        END
      ) as franja,
      flt as num_vuelo,
      DATE_DIFF(PARSE_DATE("%F", Dept_Date), PARSE_DATE("%F", scheffdate), DAY) as AP
    FROM `desa-cli-aa360.IORM_MINUTA_DISPO.MINUTA_DISPO_BI`
    WHERE
      Orgn IN (SELECT airport_code FROM airports) AND
      Dstn IN (SELECT airport_code FROM airports) AND
      PARSE_DATE("%F", Dept_Date) >= DATE('{0}') AND
      PARSE_DATE("%F", Dept_Date) <= DATE('{1}')
  ) as table_fo
  LEFT JOIN
  (
    SELECT
    observation_date,
    origin,
    destination,
    flight_no,
    departure_date,
    MAX(price) AS price
  FROM
  (

      -- oneway data

      SELECT
        observation_date,
        origin,
        destination,
        outbound_flight_no AS flight_no,
        outbound_departure_date AS departure_date,
        price_exc AS price
      FROM
        `desa-cli-aa360.IORM_INFARE_RM.FT_INFARE_{2}`
      WHERE
        source IN ('LA', 'LATAM', 'LATAM (2nd Daily)', 'LATAM (3rd Daily)')
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
        price_outbound AS price
      FROM
        `desa-cli-aa360.IORM_INFARE_RM.FT_INFARE_{2}`
      WHERE
        source IN ('LA', 'LATAM', 'LATAM (2nd Daily)', 'LATAM (3rd Daily)')
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
        price_inbound AS price
      FROM
        `desa-cli-aa360.IORM_INFARE_RM.FT_INFARE_{2}`
      WHERE
        source IN ('LA', 'LATAM', 'LATAM (2nd Daily)', 'LATAM (3rd Daily)')
      AND
        outbound_travel_stop_over is null
      AND
        inbound_travel_stop_over is null
      AND
        price_outbound IS NOT NULL
      AND
        is_one_way = 0
    )

    GROUP BY
      observation_date,
      origin,
      destination,
      departure_date,
      flight_no

    ORDER BY
      origin,
      destination,
      flight_no,
      departure_date
  ) as table_infare
  ON
    table_fo.fec_vuelo = table_infare.departure_date AND
    table_fo.fec_vista = table_infare.observation_date AND
    table_fo.ruta = CONCAT(table_infare.origin, table_infare.destination) AND
    CAST(table_fo.num_vuelo as STRING) = SUBSTR(table_infare.flight_no, 3, LENGTH(table_infare.flight_no))
  WHERE
    AP <= {3}
  ORDER BY
    num_vuelo,
    fec_vuelo,
    franja,
    AP
  )
  GROUP BY
      ruta,
      fec_vuelo,
      outbound_departure_time,
      franja,
      num_vuelo
  '''.format(DATE1, DATE2, COUNTRY, MAX_AP + STEPS)
  return query