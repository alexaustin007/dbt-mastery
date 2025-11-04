WITH base_routes AS (
      SELECT

          origin_airport,
          destination_airport,
          origin_country,
          destination_country,
          origin_region,
          destination_region,

          distance_km,
          stops,
          aircraft_type,
          codeshare,
          flight_number,
          airline_code,
          airline_name,

          flight_date,
          flight_year,
          flight_month,
          flight_quarter
      FROM {{ ref('stg_flights') }}
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