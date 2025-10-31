# dbt_mastery

A dbt project for learning and practicing data transformations with Snowflake.

## Project Structure

- `models/staging/` - Raw data cleaning and standardization
- `models/intermediate/` - Business logic and joins
- `models/marts/` - Analytics-ready tables organized by domain
- `seeds/` - CSV files for reference data
- `snapshots/` - Historical tracking of dimension tables
- `tests/` - Custom data quality tests
- `macros/` - Reusable SQL functions
- `analyses/` - Ad-hoc exploratory queries

## Getting Started

1. Install dbt: `pip install dbt-snowflake`
2. Configure your Snowflake connection in `~/.dbt/profiles.yml`
3. Test connection: `dbt debug`
4. Run models: `dbt run`
5. Run tests: `dbt test`

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Slack Community](https://www.getdbt.com/community/)
