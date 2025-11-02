  WITH origin_airports AS (
      SELECT DISTINCT
          origin_airport AS airport_code,
          origin_city AS city,
          origin_country AS country,
          origin_region AS region,
          origin_latitude AS latitude,
          origin_longitude AS longitude
      FROM {{ ref('stg_flights') }}
      WHERE origin_airport IS NOT NULL
  ),

  destination_airports AS (
      SELECT DISTINCT
          destination_airport AS airport_code,
          destination_city AS city,
          destination_country AS country,
          destination_region AS region,
          destination_latitude AS latitude,
          destination_longitude AS longitude
      FROM {{ ref('stg_flights') }}
      WHERE destination_airport IS NOT NULL
  ),

  all_airports AS (
      SELECT * FROM origin_airports
      UNION
      SELECT * FROM destination_airports
  ),

  final AS (
      SELECT
          MD5(airport_code) AS airport_key,
          airport_code,
          city,
          country,
          region,
          latitude,
          longitude
      FROM all_airports
  )

  SELECT * FROM final