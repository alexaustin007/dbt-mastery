{% snapshot snap_airlines %}

  {{
      config(
        target_schema='snapshots',
        unique_key='airline_code',

        strategy='check',
        check_cols=['airline_name']
      )
  }}

  SELECT
      airline_code,
      airline_name
  FROM {{ ref('int_airlines') }}

  {% endsnapshot %}