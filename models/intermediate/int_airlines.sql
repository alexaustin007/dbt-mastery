WITH unique_airlines AS (
    SELECT DISTINCT
        airline_code,
        airline_name
    FROM {{ ref('stg_flights') }}
    WHERE airline_code IS NOT NULL
),

final AS (
    SELECT
        MD5(airline_code) AS airline_key,
        airline_code,
        airline_name
    FROM unique_airlines
)

SELECT * FROM final
