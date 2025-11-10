{{ config(
      materialized='incremental',
      unique_key='flight_key'
  ) }}

WITH flights_base AS (
      SELECT
          flight_number,
          flight_date,
          flight_year,
          flight_month,
          flight_quarter,

          origin_airport,
          destination_airport,

          airline_code,

          distance_km,
          seats,
          aircraft_type,
          codeshare,
          stops,

          origin_city,
          origin_country,
          origin_region,
          origin_latitude,
          origin_longitude,
          destination_city,
          destination_country,
          destination_region,
          destination_latitude,
          destination_longitude

      FROM {{ ref('stg_flights') }}
  ),

  enriched_routes AS (
      SELECT
          origin_airport,
          destination_airport,
          distance_category,
          route_type,
          region_pair,
          flight_type,
          route_id
      FROM {{ ref('int_routes_enriched') }}
  ),

  origin_airports AS (
      SELECT
          airport_key AS origin_airport_key,
          airport_code AS origin_airport_code
      FROM {{ ref('int_airports') }}
  ),

  destination_airports AS (
      SELECT
          airport_key AS destination_airport_key,
          airport_code AS destination_airport_code
      FROM {{ ref('int_airports') }}
  ),

  airlines AS (
      SELECT
          airline_key,
          airline_code,
          airline_name
      FROM {{ ref('int_airlines') }}
  ),

  final AS (
      SELECT
          MD5(CONCAT(
              fb.flight_number,
              fb.origin_airport,
              fb.destination_airport,
              fb.flight_date
          )) AS flight_key,

          oa.origin_airport_key,
          da.destination_airport_key,
          a.airline_key,

          fb.flight_number,
          fb.flight_date,
          fb.flight_year,
          fb.flight_month,
          fb.flight_quarter,

          fb.origin_airport,
          fb.origin_city,
          fb.origin_country,
          fb.origin_region,
          fb.origin_latitude,
          fb.origin_longitude,

          fb.destination_airport,
          fb.destination_city,
          fb.destination_country,
          fb.destination_region,
          fb.destination_latitude,
          fb.destination_longitude,

          fb.airline_code,
          a.airline_name,

          fb.distance_km,
          fb.seats,
          fb.aircraft_type,
          fb.codeshare,
          fb.stops,

          er.distance_category,
          er.route_type,
          er.region_pair,
          er.flight_type,
          er.route_id

      FROM flights_base fb

      LEFT JOIN enriched_routes er
          ON fb.origin_airport = er.origin_airport
          AND fb.destination_airport = er.destination_airport

      LEFT JOIN origin_airports oa
          ON fb.origin_airport = oa.origin_airport_code

      LEFT JOIN destination_airports da
          ON fb.destination_airport = da.destination_airport_code


      LEFT JOIN airlines a
          ON fb.airline_code = a.airline_code
          {% if is_incremental() %}
          WHERE fb.flight_date > (SELECT MAX(flight_date) FROM {{ this }})
          {% endif %}
  )


  SELECT * FROM final