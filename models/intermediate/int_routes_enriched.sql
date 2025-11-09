  WITH base_routes AS (
      SELECT
          origin_airport,
          destination_airport,
          origin_country,
          destination_country,
          origin_region,
          destination_region,

          MIN(distance_km) as distance_km,
          MIN(stops) as stops
      FROM {{ ref('stg_flights') }}
      GROUP BY
          origin_airport,
          destination_airport,
          origin_country,
          destination_country,
          origin_region,
          destination_region
  ),

  enriched AS (
      SELECT
          *,
          
          CASE
              WHEN distance_km < 1500 THEN 'Short-haul'
              WHEN distance_km >= 1500 AND distance_km < 4000 THEN 'Medium-haul'
              WHEN distance_km >= 4000 THEN 'Long-haul'
              ELSE 'Unknown'
          END AS distance_category,

          CASE
              WHEN origin_country = destination_country THEN 'Domestic'
              ELSE 'International'
          END AS route_type,

          CONCAT(origin_region, ' to ', destination_region) AS region_pair,

          CASE
              WHEN stops = 0 THEN 'Direct'
              ELSE 'With Stops'
          END AS flight_type,

          CONCAT(origin_airport, '-', destination_airport) AS route_id

      FROM base_routes
  )

  SELECT * FROM enriched