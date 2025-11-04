WITH route_stats AS (
      SELECT

          origin_airport,
          destination_airport,
          CONCAT(origin_airport, '-', destination_airport) AS route_id,

          MAX(origin_city) AS origin_city,
          MAX(origin_country) AS origin_country,
          MAX(origin_region) AS origin_region,

          MAX(destination_city) AS destination_city,
          MAX(destination_country) AS destination_country,
          MAX(destination_region) AS destination_region,

          COUNT(*) AS total_flights,
          COUNT(DISTINCT airline_code) AS number_of_airlines,
          AVG(distance_km) AS avg_distance_km,
          AVG(seats) AS avg_seats,
          SUM(seats) AS total_seats_offered,

          MAX(aircraft_type) AS most_common_aircraft

      FROM {{ ref('stg_flights') }}
      GROUP BY
          origin_airport,
          destination_airport
  ),

  final AS (
      SELECT
          MD5(route_id) AS route_key,
          route_id,
          origin_airport,
          origin_city,
          origin_country,
          origin_region,
          destination_airport,
          destination_city,
          destination_country,
          destination_region,
          total_flights,
          number_of_airlines,
          ROUND(avg_distance_km, 2) AS avg_distance_km,
          ROUND(avg_seats, 0) AS avg_seats,
          total_seats_offered,
          most_common_aircraft,
        
          CASE
              WHEN number_of_airlines = 1 THEN 'Monopoly'
              WHEN number_of_airlines = 2 THEN 'Duopoly'
              WHEN number_of_airlines BETWEEN 3 AND 5 THEN 'Competitive'
              WHEN number_of_airlines > 5 THEN 'Highly Competitive'
          END AS competition_level

      FROM route_stats
  )

  SELECT * FROM final