
SELECT
      -- Airline information
      airline_code,
      airline_name,
      flight_number,

      -- Origin details
      origin_airport,
      origin_city,
      origin_country,
      origin_region,
      origin_latitude,
      origin_longitude,

      -- Destination details
      destination_airport,
      destination_city,
      destination_country,
      destination_region,
      destination_latitude,
      destination_longitude,

      -- Flight details
      distance_km,
      seats,
      aircraft_type,
      codeshare,
      stops,

      -- Date information
      flight_date,
      flight_year,
      flight_month,
      flight_quarter
FROM {{source('raw', 'raw_flights_data')}}
WHERE flight_date IS NOT NULL