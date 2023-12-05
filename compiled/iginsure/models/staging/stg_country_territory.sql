

with

zone_territory as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_zone_territory" where _valid_to is null
),

zone_country as (
    select * from "IGI_PROD_DW"."dbt_dev"."scd_zone_country" where _valid_to is null
),

country_territory as (
      select
          zone_country."lCountryKey" as country_id,
          zone_country."sCountry" as country,
          zone_territory.STERRITORY as territory
      from
          zone_country
      left join zone_territory on
          zone_country.LTERRITORYKEY = zone_territory.LTERRITORYKEY
)

select * from country_territory